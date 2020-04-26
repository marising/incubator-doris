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

package org.apache.doris.common.util;

import com.google.common.base.Preconditions;

import java.nio.charset.StandardCharsets;

/**
 * Common String utilities for Doris
 */
public class StringUtils {
    /**
     * Get UTF 8 bytes from input string
     * @param str
     * @return
     */
    public static byte[] toUtf8(String str){
        Preconditions.checkNotNull(str, "Input String for UTF8 conversion is null!");
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Normalize input string to be valid as http header value. For instance, it is illegal to contain "\r", "\n"
     * @param str
     * @return
     */
    public static String normalizeForHttp(String str){
        Preconditions.checkNotNull(str, "Input String for Http Header normalization is null!");
        return str.replaceAll("\r|\n", "");
    }
}