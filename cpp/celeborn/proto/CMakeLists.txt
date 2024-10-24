add_library(proto STATIC ${ProtoGenSource} ${ProtoGenHeader})
target_link_libraries(proto ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY})