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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import rx.functions.Func1;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class HttpBodyLogger implements Func1<Buffer, Buffer> {

    private static final Logger LOGGER = getLogger(HttpBodyLogger.class);

    @Override
    public Buffer call(Buffer buffer) {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\r\nHttp Body Dump >>>>>\r\n\r\n");
            sb.append(buffer.toString(UTF_8.toString()));
            sb.append("\r\n\r\nHttp Body Dump <<<<<\r\n");
            LOGGER.debug(sb.toString());
        }
        return buffer;
    }
}
