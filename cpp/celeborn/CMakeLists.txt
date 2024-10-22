include_directories(${CMAKE_BINARY_DIR})

add_subdirectory(proto)
add_subdirectory(utils)
add_subdirectory(memory)
add_subdirectory(network)
add_subdirectory(protocol)
add_subdirectory(client)
add_subdirectory(conf)

add_subdirectory(example)