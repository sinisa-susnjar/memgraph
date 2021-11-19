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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/impl/codegen/call_op_set.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "memgraph.grpc.pb.h"
#include "memgraph.pb.h"

#define PRINT 0

#ifndef NDEBUG
#define PRINT 0
#endif

using grpc::CallbackServerContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerUnaryReactor;
using grpc::ServerWriteReactor;
using grpc::Status;
using SyncServerOption = grpc::ServerBuilder::SyncServerOption;
using memgraph::List;
using memgraph::PropertyRequest;
using memgraph::PropertyValue;
using memgraph::Storage;
using std::chrono::system_clock;

class StorageImpl final : public Storage::CallbackService {
 public:
  grpc::ServerUnaryReactor *GetProperty(CallbackServerContext *context, const PropertyRequest *request,
                                        PropertyValue *reply) override {
#if PRINT
    std::cout << "Request received " << request->name() << '\n';
#endif
    // The actual processing.
    reply->set_string_v("Property name " + request->name());

#if PRINT
    std::cout << "Sending reply " << reply->string_v() << '\n';
#endif
    auto *reactor = context->DefaultReactor();
    reactor->Finish(Status::OK);
    return reactor;
  }

  ServerWriteReactor<PropertyValue> *GetPropertyStream(CallbackServerContext *context,
                                                       const PropertyRequest *request) override {
    class PropertyStreamer : public ServerWriteReactor<PropertyValue> {
     public:
      explicit PropertyStreamer(const PropertyRequest &request)
          : name_{request.name()}, expected_message_count_(request.has_count() ? request.count() : 1) {
        NextWrite();
      }
      void OnDone() override { delete this; }
      void OnWriteDone(bool /*ok*/) override { NextWrite(); }

     private:
      void NextWrite() {
        if (sent_message_count_ == expected_message_count_) {
#if PRINT
          std::cout << " Finished\n";
#endif
          // Didn't write anything, all is done.
          Finish(Status::OK);
          return;
        }
        reply_.set_string_v("Property name " + name_ + " #" + std::to_string(sent_message_count_++));

#if PRINT
        std::cout << "Sending reply " << reply_.string_v() << '\n';
#endif
        StartWrite(&reply_);
      }
      std::string name_;
      int64_t expected_message_count_;
      int64_t sent_message_count_{0};
      PropertyValue reply_;
    };
    return new PropertyStreamer(*request);
  }

  grpc::ServerUnaryReactor *GetPropertyStream2(CallbackServerContext *context, const PropertyRequest *request,
                                               List *reply) override {
#if PRINT
    std::cout << "Request received " << request->name() << '\n';
#endif
    const auto expected_message_count = request->has_count() ? request->count() : 1;
    const auto &name = request->name();
    for (auto i{0}; i < expected_message_count; ++i) {
      reply->add_list()->set_string_v("Property name " + name);
    }

#if PRINT
    std::cout << "Sending reply " << reply->string_v() << '\n';
#endif
    auto *reactor = context->DefaultReactor();
    reactor->Finish(Status::OK);
    return reactor;
  }
};

void RunServer(const std::string &db_path) {}

int main() {
  std::string server_address("0.0.0.0:50051");
  StorageImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials())
      .RegisterService(&service)
      .SetSyncServerOption(SyncServerOption::NUM_CQS, 4)
      .SetSyncServerOption(SyncServerOption::MAX_POLLERS, 8);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();

  return 0;
}
