// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.jd.olap.load.common;

public class OutputBuffer {
  private final String stdout;
  private final String stderr;

  /**
   * Create an OutputBuffer, a class for storing and managing stdout and stderr
   * results separately
   *
   * @param stdout stdout result
   * @param stderr stderr result
   */
  public OutputBuffer(String stdout, String stderr) {
    this.stdout = stdout;
    this.stderr = stderr;
  }

  /**
   * Returns the stdout result
   *
   * @return stdout result
   */
  public String getStdout() {
    return stdout;
  }

  /**
   * Returns the stderr result
   *
   * @return stderr result
   */
  public String getStderr() {
    return stderr;
  }
}
