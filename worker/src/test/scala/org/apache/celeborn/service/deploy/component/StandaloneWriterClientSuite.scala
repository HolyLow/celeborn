package org.apache.celeborn.service.deploy.component

// accept:
//  - the ip (host + port) of lifecylemanager
//  - the appId, shuffleId
//  - the #reducers
//  - the outputFile of the sum result
// do:
//  - read the data from the lifecyclemanager's shuffleClient, in the form of "data0-data1-data2-..."
//  - calculate the sum result for each reducer, and write into the outputFile in form "sumOfReducer0-sumOfReducer1-..."

class StandaloneWriterClientSuite {}
