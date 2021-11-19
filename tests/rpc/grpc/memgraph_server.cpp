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

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "memgraph.grpc.pb.h"
#include "memgraph.pb.h"

#define PRINT 0

#ifndef NDEBUG
#define PRINT 0
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;
using memgraph::List;
using memgraph::PropertyRequest;
using memgraph::PropertyValue;
using memgraph::Storage;

// Logic and data behind the server's behavior.
class MemgraphServiceImpl final : public Storage::Service {
  Status GetProperty(ServerContext *context, const PropertyRequest *request, PropertyValue *reply) override {
#if PRINT
    std::cout << "Request received " << request->name() << '\n';
#endif
    reply->set_string_v("Property name " + request->name());

#if PRINT
    std::cout << "Sending reply " << reply->string_v() << '\n';
#endif
    return Status::OK;
  }

  Status GetPropertyStream(ServerContext *context, const PropertyRequest *request,
                           ServerWriter<PropertyValue> *writer) override {
    constexpr auto kDefaultMessageCount = 1;
    const auto expected_message_count = request->has_count() ? request->count() : kDefaultMessageCount;
#if PRINT
    std::cout << "Request received " << request->name() << ", sending " << expected_message_count << " messages\n";
#endif
    PropertyValue reply;
    for (auto i{0}; i < expected_message_count; ++i) {
      reply.set_string_v("Property name " + request->name() + " #" + std::to_string(i));

#if PRINT
      std::cout << "Sending reply " << reply.string_v() << '\n';
#endif
      writer->Write(reply);
    }
    return Status::OK;
  }

  Status GetPropertyStream2(ServerContext *context, const PropertyRequest *request, List *reply) override {
    constexpr auto kDefaultMessageCount = 1;
    const auto expected_message_count = request->has_count() ? request->count() : kDefaultMessageCount;
#if PRINT
    std::cout << "Request received " << request->name() << '\n';
#endif

    for (auto i{0}; i < expected_message_count; ++i) {
      reply->add_list()->set_string_v("Property name " + request->name() + " #" + std::to_string(i));
    }

#if PRINT
    std::cout << "Sending reply " << reply->string_v() << '\n';
#endif
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  MemgraphServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char **argv) {
  RunServer();

  return 0;
}
