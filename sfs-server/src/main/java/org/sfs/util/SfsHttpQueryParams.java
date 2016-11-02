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

import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.UNICODE_CASE;
import static java.util.regex.Pattern.compile;

public class SfsHttpQueryParams {

    public static final String DESTROY = "destroy";
    public static final String KEEP_ALIVE_TIMEOUT = "keep_alive_timeout";
    public static final String TEMP_URL_EXPIRES = "temp_url_expires";
    public static final String TEMP_URL_SIG = "temp_url_sig";
    public static final String MULTIPART_MANIFEST = "multipart-manifest";
    public static final String LIMIT = "limit";
    public static final String MARKER = "marker";
    public static final String END_MARKER = "end_marker";
    public static final String FORMAT = "format";
    public static final String PREFIX = "prefix";
    public static final String DELIMITER = "delimiter";
    public static final String NODE = "node";
    public static final String VOLUME = "volume";
    public static final String POSITION = "position";
    public static final String LENGTH = "length";
    public static final String VERSION = "version";
    public static final String OFFSET = "offset";
    public static final Pattern COMPUTED_DIGEST = compile("^X-Computed-Digest-(.+)$", CASE_INSENSITIVE | UNICODE_CASE);
    public static final String X_CONTENT_COMPUTED_DIGEST_PREFIX = SfsHttpHeaders.X_CONTENT_COMPUTED_DIGEST_PREFIX;

}
