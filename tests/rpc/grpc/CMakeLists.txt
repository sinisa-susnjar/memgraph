# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()


get_filename_component(memgraph_proto "./memgraph.proto" ABSOLUTE)
get_filename_component(memgraph_proto_path "${memgraph_proto}" PATH)

set(memgraph_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/memgraph.pb.cc")
set(memgraph_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/memgraph.pb.h")
set(memgraph_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/memgraph.grpc.pb.cc")
set(memgraph_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/memgraph.grpc.pb.h")

add_custom_command(
      OUTPUT "${memgraph_proto_srcs}" "${memgraph_proto_hdrs}" "${memgraph_grpc_srcs}" "${memgraph_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${memgraph_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${memgraph_proto}"
      DEPENDS "${memgraph_proto}")


# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# memgraph_grpc_proto
add_library(memgraph_grpc_proto
  ${memgraph_grpc_srcs}
  ${memgraph_grpc_hdrs}
  ${memgraph_proto_srcs}
  ${memgraph_proto_hdrs})
target_link_libraries(memgraph_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Targets memgraph_[async_](client|server)
foreach(_target
  memgraph_async_client
  memgraph_async_server)
  add_executable(${_target} "${_target}.cpp")
  target_link_libraries(${_target}
    memgraph_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()
