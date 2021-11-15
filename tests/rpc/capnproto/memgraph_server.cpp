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

#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <iostream>
#include "memgraph.capnp.h"
class StorageImpl final : public Storage::Server {
  // Implementation of the Calculator Cap'n Proto interface.

 public:
  kj::Promise<void> getProperty(GetPropertyContext context) override {
#if PRINT
    std::cout << "Request received " << request->name() << '\n';
#endif
    std::string name = context.getParams().getReq().getName();
    context.getResults().initProperty().setStringV("Property name " + name);

#if PRINT
    std::cout << "Sending reply " << reply->string_v() << '\n';
#endif
    return kj::READY_NOW;
  }

  kj::Promise<void> getPropertyStream(GetPropertyStreamContext context) override {
    constexpr auto kDefaultMessageCount = 1;

    const auto &req = context.getParams().getReq();
    const auto expected_message_count = [&req]() {
      const auto count = req.getCount();
      return count <= 0 ? kDefaultMessageCount : count;
    }();
#if PRINT
    std::cout << "Request received " << request->name() << ", sending " << expected_message_count << " messages\n";
#endif
    std::string name = context.getParams().getReq().getName();
    auto properties = context.getResults().initProperties(expected_message_count);
    for (auto i{0}; i < expected_message_count; ++i) {
      properties[i].setStringV("Property name " + name + " #" + std::to_string(i));

#if PRINT
      std::cout << "Sending reply " << reply.string_v() << '\n';
#endif
    }
    return kj::READY_NOW;
  }
};

int main(int argc, const char *argv[]) {
  if (argc != 2) {
    std::cerr << "usage: " << argv[0]
              << " ADDRESS[:PORT]\n"
                 "Runs the server bound to the given address/port.\n"
                 "ADDRESS may be '*' to bind to all local addresses.\n"
                 ":PORT may be omitted to choose a port automatically."
              << std::endl;
    return 1;
  }

  // Set up a server.
  capnp::EzRpcServer server(kj::heap<StorageImpl>(), argv[1]);

  // Write the port number to stdout, in case it was chosen automatically.
  auto &waitScope = server.getWaitScope();
  uint port = server.getPort().wait(waitScope);
  if (port == 0) {
    // The address format "unix:/path/to/socket" opens a unix domain socket,
    // in which case the port will be zero.
    std::cout << "Listening on Unix socket..." << std::endl;
  } else {
    std::cout << "Listening on port " << port << "..." << std::endl;
  }

  // Run forever, accepting connections and handling requests.
  kj::NEVER_DONE.wait(waitScope);
}
