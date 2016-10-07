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

import java.io.UnsupportedEncodingException;

import static com.google.common.base.Charsets.UTF_8;
import static java.net.URLDecoder.decode;
import static java.net.URLEncoder.encode;

public class UrlScaper {

    public static String unescape(String value) {
        if (value != null) {
            try {
                return decode(value, UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return value;
    }

    public static String escape(String value) {
        if (value != null) {
            try {
                return encode(value, UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        return value;
    }

}
