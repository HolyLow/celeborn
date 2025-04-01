package org.apache.celeborn.service.deploy.component

// accept:
//  - the ip (host + port) of lifecylemanager
//  - the appId, the shuffleId
//  - the #mappers, #reducers
//  - the intDataNum
//  - the outputFile of the reference sum result
// do:
//  - generate the #intDataNum for each mapper
//  - write the data into the lifecyclemanager's shuffleClient, in the form of "data0-data1-data2-..."
//  - calculate the reference result for each reducer, and write into the outputFile in form "sumOfReducer0-sumOfReducer1-..."
class StandaloneReaderClientSuite {}
