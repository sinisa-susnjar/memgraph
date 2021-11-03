// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "memgraph.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerAsyncWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::WriteOptions;
using memgraph::PropertyRequest;
using memgraph::PropertyValue;
using memgraph::Storage;

class MemgraphServerImpl final {
 public:
  ~MemgraphServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Start() {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;
  }

  // This can be run in multiple threads if needed.
  void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
    new SingleCallData(&service_, cq_.get());
    new StreamCallData(&service_, cq_.get());
    void *tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      CallData::detag(tag).Proceed();
    }
  }

 private:
  struct CallData {
    virtual ~CallData(){};
    static void *tag(CallData &tag) { return static_cast<void *>(&tag); }
    static CallData &detag(void *tag) { return *static_cast<CallData *>(tag); }
    virtual void Proceed() = 0;
  };

  // Class encompasing the state and logic needed to serve a request.
  class SingleCallData : public CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    SingleCallData(Storage::AsyncService *service, ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CallStatus::CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed() override {
      if (status_ == CallStatus::CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = CallStatus::PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestGetProperty(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == CallStatus::PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new SingleCallData(service_, cq_);

#if !defined(NDEBUG)
        std::cout << "Request received " << request_.name() << '\n';
#endif
        // The actual processing.
        reply_.set_string_v("Property name " + request_.name());

#if !defined(NDEBUG)
        std::cout << "Sending reply " << reply_.string_v() << '\n';
#endif

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        status_ = CallStatus::FINISH;
        responder_.Finish(reply_, Status::OK, this);
      } else {
        GPR_ASSERT(status_ == CallStatus::FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Storage::AsyncService *service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue *cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    PropertyRequest request_;
    // What we send back to the client.
    PropertyValue reply_;

    // The means to get back to the client.
    ServerAsyncResponseWriter<PropertyValue> responder_;

    // Let's implement a tiny state machine with the following states.
    enum class CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
  };

  class StreamCallData : public CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    StreamCallData(Storage::AsyncService *service, ServerCompletionQueue *cq)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CallStatus::CREATE) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed() override {
      if (status_ == CallStatus::CREATE) {
        // Make this instance progress to the PROCESS state.
        status_ = CallStatus::PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.
        service_->RequestGetPropertyStream(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == CallStatus::PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new StreamCallData(service_, cq_);

        if (request_.has_count() && request_.count() >= 0) {
          expected_message_count_ = request_.count();
        }

#if !defined(NDEBUG)
        std::cout << "Request received " << request_.name() << ", sending " << expected_message_count_ << " messages\n";
#endif
        SendMessage();
      } else if (status_ == CallStatus::PROCESSING) {
        SendMessage();
      } else {
        GPR_ASSERT(status_ == CallStatus::FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    void SendMessage() {
      if (replies_sent_ >= expected_message_count_) {
        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
#if !defined(NDEBUG)
        std::cout << " Finished\n";
#endif

        status_ = CallStatus::FINISH;
        responder_.Finish(Status::OK, tag(*this));
        return;
      }
      reply_.set_string_v("Property name " + request_.name() + " #" + std::to_string(replies_sent_++));

#if !defined(NDEBUG)
      std::cout << "Sending reply " << reply_.string_v() << '\n';
#endif

      status_ = CallStatus::PROCESSING;
      responder_.Write(reply_, tag(*this));
    }
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Storage::AsyncService *service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue *cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;

    // What we get from the client.
    PropertyRequest request_;
    // What we send back to the client.
    PropertyValue reply_;

    // The means to get back to the client.
    ServerAsyncWriter<PropertyValue> responder_;

    // Let's implement a tiny state machine with the following states.
    enum class CallStatus { CREATE, PROCESS, PROCESSING, FINISH };
    CallStatus status_;  // The current serving state.
    int64_t replies_sent_{0};
    static constexpr auto kDefaultReply = 1;
    int64_t expected_message_count_{kDefaultReply};
  };

  std::unique_ptr<ServerCompletionQueue> cq_;
  Storage::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char **argv) {
  MemgraphServerImpl server;
  server.Start();

  // Spawn reader thread that loops indefinitely
  std::vector<std::jthread> threads{};
  for (auto i = 0; i < 8; ++i) {
    threads.emplace_back(&MemgraphServerImpl::HandleRpcs, &server);
  }

  return 0;
}
