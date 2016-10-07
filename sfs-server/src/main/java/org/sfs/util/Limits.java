/*
 * Copyright 2016 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.util;

public class Limits {

    public static int NOT_SET = -1;
    public static final long MAX_SEGMENT_SIZE = 5L * 1024 * 1024 * 1024;
    public static final int MAX_AUTH_REQUEST_SIZE = 1024 * 1024;
    public static final int MAX_OBJECT_REVISIONS = 0;
}
