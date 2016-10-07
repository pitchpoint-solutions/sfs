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

import com.google.common.base.Optional;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static java.security.MessageDigest.getInstance;

public enum MessageDigestFactory {

    SHA512("SHA-512"),
    SHA256("SHA-256"),
    MD5("MD5");

    /*
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
    private final String value;

    MessageDigestFactory(String value) {
        this.value = value;
    }

    public static Optional<MessageDigestFactory> fromValueIfExists(String value) {
        if (value != null) {
            for (MessageDigestFactory factory : values()) {
                if (factory.value.equalsIgnoreCase(value)) {
                    return of(factory);
                }
            }
        }
        return absent();
    }

    public String getValue() {
        return value;
    }

    public MessageDigest instance() {
        try {
            return getInstance(getValue());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
