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

#include <atomic>
#include <chrono>
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

using grpc::Channel;
using grpc::ClientAsyncReader;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using memgraph::List;
using memgraph::PropertyRequest;
using memgraph::PropertyValue;
using memgraph::Storage;

class StorageClient {
 public:
  explicit StorageClient(std::shared_ptr<Channel> channel) : stub_(Storage::NewStub(channel)) {}

  // Assembles the client's payload and sends it to the server.
  void GetProperty(const std::string &name) {
    // Data we are sending to the server.
    PropertyRequest request;
    request.set_name(name);

    // Call object to store rpc data
    auto *call = new GetPropertyCall;
    call->received_message_count_ = &received_message_count_;

    // stub_->AsyncGetProperty() creates an RPC object, returning
    // an instance to store in "call" and starts the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->response_reader = stub_->AsyncGetProperty(&call->context, request, &cq_);

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call
    // object.

    call->response_reader->Finish(&call->reply, &call->status, ResponseHandler::tag(*call));
  }

  void GetProperties(const std::string &name, const int64_t count) {
    // Data we are sending to the server.
    PropertyRequest request;
    request.set_name(name);
    request.set_count(count);

    // Call object to store rpc data
    auto *call = new GetPropertyStreamCall();
    call->received_message_count_ = &received_message_count_;

    // stub_->AsyncGetProperty() creates an RPC object, returning
    // an instance to store in "call" and starts the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->response_reader = stub_->PrepareAsyncGetPropertyStream(&call->context, request, &cq_);
    call->response_reader->StartCall(ResponseHandler::tag(*call));
  }

  void GetProperties2(const std::string &name, const int64_t count) {
    // Data we are sending to the server.
    PropertyRequest request;
    request.set_name(name);
    request.set_count(count);

    // Call object to store rpc data
    auto *call = new GetPropertyStreamCall2();
    call->received_message_count_ = &received_message_count_;

    // stub_->AsyncGetProperty() creates an RPC object, returning
    // an instance to store in "call" and starts the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->response_reader = stub_->AsyncGetPropertyStream2(&call->context, request, &cq_);

    call->response_reader->Finish(&call->reply, &call->status, ResponseHandler::tag(*call));
  }

  // Loop while listening for completed responses.
  // Prints out the response from the server.
  void AsyncCompleteRpc() {
    void *got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (cq_.Next(&got_tag, &ok)) {
      // The tag in this example is the memory location of the callback
      ResponseHandler::detag(got_tag).HandleResponse(ok);
    }
  }

  uint64_t ReceivedMessageCount() const { return received_message_count_.load(); }

 private:
  class ResponseHandler {
   public:
    virtual ~ResponseHandler(){};
    static void *tag(ResponseHandler &tag) { return static_cast<void *>(&tag); }
    static ResponseHandler &detag(void *tag) { return *static_cast<ResponseHandler *>(tag); }

    virtual void HandleResponse(bool eventStatus) = 0;
  };

  // struct for keeping state and data information
  struct CommonCall {
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // Storage for the status of the RPC upon completion.
    Status status;

    // Container for the data we expect from the server.
    std::atomic<uint64_t> *received_message_count_;
  };

  struct GetPropertyCall : public ResponseHandler, public CommonCall {
   public:
    std::unique_ptr<ClientAsyncResponseReader<PropertyValue>> response_reader;
    PropertyValue reply;
    void HandleResponse(bool ok) override {
      // Verify that the request was completed successfully. Note that "ok"
      // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);

      if (status.ok()) {
#if PRINT
        std::cout << "Client received: " << reply.string_v() << std::endl;
#endif
        ++*received_message_count_;
      } else {
        std::cout << "RPC failed" << std::endl;
      }

      // Once we're complete, deallocate the call object.
      delete this;
    }
  };

  class GetPropertyStreamCall : public ResponseHandler, public CommonCall {
   public:
    std::unique_ptr<ClientAsyncReader<PropertyValue>> response_reader;
    PropertyValue reply;

    void HandleResponse(bool ok) override {
      switch (call_status) {
        case CallStatus::CREATE:
          if (ok) {
            call_status = CallStatus::PROCESS;
            response_reader->Read(&reply, (void *)this);
          } else {
            call_status = CallStatus::FINISH;
            response_reader->Finish(&status, (void *)this);
          }
          break;
        case CallStatus::PROCESS:
          if (ok) {
#if PRINT
            std::cout << "Client received: " << reply.string_v() << std::endl;
#endif
            response_reader->Read(&reply, (void *)this);
            ++*received_message_count_;
          } else {
            call_status = CallStatus::FINISH;
            response_reader->Finish(&status, (void *)this);
          }
          break;
        case CallStatus::FINISH:
          if (status.ok()) {
#if PRINT
            std::cout << "Server Response Completed: " << this << " CallData: " << this << std::endl;
#endif
          } else {
            std::cout << "RPC failed\n" << status.error_message() << std::endl;
          }
          delete this;
      }
    }

   private:
    enum class CallStatus { CREATE, PROCESS, FINISH };
    CallStatus call_status{CallStatus::CREATE};
  };

  struct GetPropertyStreamCall2 : public ResponseHandler, public CommonCall {
   public:
    std::unique_ptr<ClientAsyncResponseReader<List>> response_reader;
    List reply;
    void HandleResponse(bool ok) override {
      // Verify that the request was completed successfully. Note that "ok"
      // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);

      if (status.ok()) {
#if PRINT
        std::cout << "Client received: " << reply.string_v() << std::endl;
#endif
        received_message_count_->fetch_add(reply.list_size());
      } else {
        std::cout << "RPC failed" << std::endl;
      }

      // Once we're complete, deallocate the call object.
      delete this;
    }
  };

  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<Storage::Stub> stub_;

  // The producer-consumer queue we use to communicate asynchronously with the
  // gRPC runtime.
  CompletionQueue cq_;
  std::atomic<uint64_t> received_message_count_{0};
};

int main(int argc, char **argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  StorageClient storage(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

  // Spawn reader thread that loops indefinitely
  std::vector<std::jthread> threads{};
  for (auto i = 0; i < 8; ++i) {
    threads.emplace_back(&StorageClient::AsyncCompleteRpc, &storage);
  }

  uint64_t expected_message_count_ = 0;

  const auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 10000; i++) {
    std::string name("world " + std::to_string(i));
    uint64_t message_count = 200;
    storage.GetProperties2(name, message_count);  // The actual RPC call!
    expected_message_count_ += message_count;
  }

  while (storage.ReceivedMessageCount() != expected_message_count_) {
    // std::cout << storage.ReceivedMessageCount() << "  " << expected_message_count_ << '\n';
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }

  const auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Done: in " << std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count()
            << " seconds\n";
  std::cout << "Press control-c to quit" << std::endl << std::endl;

  return 0;
}
