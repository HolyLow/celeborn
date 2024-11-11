/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <google/protobuf/io/coded_stream.h>
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
template <typename T>
std::unique_ptr<T> parseProto(const uint8_t* bytes, int len) {
  CELEBORN_CHECK_NOT_NULL(
      bytes, "Data for {} must be non-null", typeid(T).name());

  auto pbObj = std::make_unique<T>();

  google::protobuf::io::CodedInputStream cis(bytes, len);

  // The default recursion depth is 100, which causes some test cases to fail
  // during regression testing. By setting the recursion depth limit to 2000,
  // it means that during the parsing process, if the recursion depth exceeds
  // 2000 layers, the parsing process will be terminated and an error will be
  // returned.
  cis.SetRecursionLimit(2000);
  bool parseSuccess = (pbObj.get())->ParseFromCodedStream(&cis);

  if (!parseSuccess) {
    std::cerr << "Unable to parse " << typeid(T).name() << " protobuf";
    exit(1);
  }
  return pbObj;
}
} // namespace celeborn
