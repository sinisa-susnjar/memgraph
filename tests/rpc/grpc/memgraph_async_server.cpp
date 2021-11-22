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

#define PRINT 0

#ifndef NDEBUG
#define PRINT 0
#endif

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerAsyncWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using grpc::WriteOptions;
using memgraph::List;
using memgraph::PropertyRequest;
using memgraph::PropertyValue;
using memgraph::Storage;

class MemgraphServerImpl final {
 public:
  ~MemgraphServerImpl() {
    for (auto &thread : threads_) {
      thread.join();
    }
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (auto &cq : cqs_) {
      cq->Shutdown();
    }
  }

  // There is no shutdown handling in this code.
  void Start(const int number_of_threads) {
    std::string server_address("0.0.0.0:50051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    for (int i = 0; i < number_of_threads; ++i) {
      cqs_.push_back(builder.AddCompletionQueue());
    }
    // Finally assemble the server.
    server_ = builder.BuildAndStart();

    // Spawn reader thread that loops indefinitely
    for (auto i = 0; i < number_of_threads; ++i) {
      threads_.emplace_back(&MemgraphServerImpl::HandleRpcs, this, i);
    }
    std::cout << "Server listening on " << server_address << std::endl;
  }

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_index) {
    // Spawn a new CallData instance to serve new clients.
    auto &cq = cqs_[cq_index];
    new SingleCallData(&service_, cq.get());
    new StreamCallData(&service_, cq.get());
    new StreamCallData2(&service_, cq.get());
    void *tag;  // uniquely identifies a request.
    bool ok;
    while (cq->Next(&tag, &ok)) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      CallData::detag(tag).Proceed();
    }
    std::cout << "Exit |" << cq_index << "|\n";
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

#if PRINT
        std::cout << "Request received " << request_.name() << '\n';
#endif
        // The actual processing.
        reply_.set_string_v("Property name " + request_.name());

#if PRINT
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

#if PRINT
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
#if PRINT
        std::cout << " Finished\n";
#endif

        status_ = CallStatus::FINISH;
        responder_.Finish(Status::OK, tag(*this));
        return;
      }
      reply_.set_string_v("Property name " + request_.name() + " #" + std::to_string(replies_sent_++));

#if PRINT
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

  class StreamCallData2 : public CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
    StreamCallData2(Storage::AsyncService *service, ServerCompletionQueue *cq)
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
        service_->RequestGetPropertyStream2(&ctx_, &request_, &responder_, cq_, cq_, this);
      } else if (status_ == CallStatus::PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new StreamCallData2(service_, cq_);

        if (request_.has_count() && request_.count() >= 0) {
          expected_message_count_ = request_.count();
        }
        SendMessages();
      } else {
        GPR_ASSERT(status_ == CallStatus::FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    void SendMessages() {
#if PRINT
      std::cout << "Request received " << request->name() << ", sending " << expected_message_count_ << " messages\n";
#endif
      for (auto i{0}; i < expected_message_count_; ++i) {
        reply_.add_list()->set_string_v("Property name " + request_.name() + " #" + std::to_string(i));
      }

      // And we are done! Let the gRPC runtime know we've finished, using the
      // memory address of this instance as the uniquely identifying tag for
      // the event.

#if PRINT
      std::cout << "Sending reply\n";
#endif

      status_ = CallStatus::FINISH;
      responder_.Finish(reply_, Status::OK, tag(*this));
#if PRINT
      std::cout << " Finished\n";
#endif
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
    List reply_;

    // The means to get back to the client.
    ServerAsyncResponseWriter<List> responder_;

    // Let's implement a tiny state machine with the following states.
    enum class CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
    static constexpr auto kDefaultReply = 1;
    int64_t expected_message_count_{kDefaultReply};
  };

  std::vector<std::unique_ptr<ServerCompletionQueue>> cqs_;
  Storage::AsyncService service_;
  std::unique_ptr<Server> server_;
  std::vector<std::jthread> threads_;
};

int main(int argc, char **argv) {
  MemgraphServerImpl server;
  server.Start(8);

  return 0;
}
