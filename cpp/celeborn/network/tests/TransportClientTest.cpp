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

#include <gtest/gtest.h>

#include "celeborn/network/TransportClient.h"

using namespace celeborn;

// test the MessageDispatcher. the underlying layer could be mocked.
// todo: consider make the MessageDispatcher a separate standalone file???
// todo: how to mock the pipeline... use a pipeline factory???

// test TransportClient. the underlying dispatcher could be mocked.
// todo: the dispatcher kind is firm... the functions are not virtual and
//  can hardly be mocked... handle by using the underlying pipeline?? or
//  make the function virtual???


// The transportClientFactory could hardly be tested... we would not
// test it in ut...


