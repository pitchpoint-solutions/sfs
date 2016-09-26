/*
 *
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

package org.sfs.util;

import com.google.common.base.Optional;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static javax.crypto.KeyGenerator.getInstance;

public enum MacDigestFactory {

    SHA512("HmacSHA512"),
    SHA256("HmacSHA256"),
    MD5("HmacMD5");

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

    MacDigestFactory(String value) {
        this.value = value;
    }

    public static Optional<MacDigestFactory> fromValueIfExists(String value) {
        if (value != null) {
            for (MacDigestFactory factory : values()) {
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

    public byte[] generateKey() {
        try {
            SecretKey key = getInstance(getValue()).generateKey();
            return key.getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public Mac instance(byte[] secretKey) {
        try {
            String v = getValue();
            Mac mac = Mac.getInstance(v);
            mac.init(new SecretKeySpec(secretKey, v));
            return mac;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }
}
