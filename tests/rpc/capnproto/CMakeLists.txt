find_package(CapnProto CONFIG REQUIRED)

capnp_generate_cpp(memgraphSources memgrahpHeaders memgraph.capnp)
add_executable(capnp_memgraph_client memgraph_client.cpp ${memgraphSources})
add_executable(capnp_memgraph_server memgraph_server.cpp ${memgraphSources})
target_link_libraries(capnp_memgraph_client PRIVATE CapnProto::capnp-rpc)
target_link_libraries(capnp_memgraph_server PRIVATE CapnProto::capnp-rpc)
target_include_directories(capnp_memgraph_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(capnp_memgraph_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
