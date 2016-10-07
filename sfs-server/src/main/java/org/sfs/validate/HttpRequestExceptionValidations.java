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

package org.sfs.validate;

import com.google.common.io.BaseEncoding;
import io.vertx.core.json.JsonObject;
import org.sfs.util.HttpRequestValidationException;

import java.util.Arrays;

import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.ETAG;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_SHA512;

public class HttpRequestExceptionValidations {

    private static void validateHeader(String headerName, byte[] expectedMd5, byte[] calculatedMd5, BaseEncoding baseEncoding) {
        if (!Arrays.equals(calculatedMd5, expectedMd5)) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", format("%s was %s, expected %s", headerName, baseEncoding.encode(calculatedMd5), baseEncoding.encode(expectedMd5)));
            throw new HttpRequestValidationException(HTTP_CONFLICT, jsonObject);
        }
    }

    public static void validateEtag(byte[] expected, byte[] calculated) {
        validateHeader(ETAG, expected, calculated, base16().lowerCase());
    }

    public static void validateContentMd5(byte[] expected, byte[] calculated) {
        validateHeader(CONTENT_MD5, expected, calculated, base64());
    }

    public static void validateContentSha512(byte[] expected, byte[] calculated) {
        validateHeader(X_CONTENT_SHA512, expected, calculated, base64());
    }

    public static void validateContentLength(long expected, long calculated) {
        if (expected != calculated) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", format("%s was %s, expected %s", CONTENT_LENGTH, calculated, expected));
            throw new HttpRequestValidationException(HTTP_CONFLICT, jsonObject);
        }
    }
}
