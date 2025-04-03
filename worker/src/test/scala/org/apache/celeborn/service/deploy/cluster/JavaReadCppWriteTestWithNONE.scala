package org.apache.celeborn.service.deploy.cluster

class JavaReadCppWriteTestWithNONE extends JavaReadCppWriteTestBase {

  test(s"test javaReadCppWrite with NONE") {
    testJavaReadCppWrite()
  }
}
