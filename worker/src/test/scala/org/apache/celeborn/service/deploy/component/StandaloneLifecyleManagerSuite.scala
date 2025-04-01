package org.apache.celeborn.service.deploy.component

// accept:
//  - the master port
//  - the appId
//  - a timeout and a termination fileName
// do:
//  - start the lifecyclemanager
//  - loop until timeout, or until the termination file is created
class StandaloneLifecyleManagerSuite {}
