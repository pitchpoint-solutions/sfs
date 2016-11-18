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

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.sfs.SfsVertx;
import org.sfs.rx.RxHelper;
import rx.Observable;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

public class PrngRandom {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrngRandom.class);
    private static volatile SecureRandom secureRandom;
    private static final AtomicLong invocationCount = new AtomicLong(0L);
    private static final long MAX_INVOCATIONS = 1000000;

    static {
        try {
            secureRandom = SecureRandom.getInstance("NativePRNGNonBlocking");
        } catch (Exception e) {
            try {
                secureRandom = SecureRandom.getInstanceStrong();
            } catch (Exception nE) {
                throw new ExceptionInInitializerError(nE);
            }
        }
        LOGGER.info("Entropy generator is " + secureRandom.getAlgorithm());
    }

    private SecureRandom getSecureRandom() {
        // reseed every MAX_INVOCATIONS so that the entropy stream is less predictable
        if (invocationCount.incrementAndGet() >= MAX_INVOCATIONS) {
            synchronized (invocationCount) {
                if (invocationCount.get() >= MAX_INVOCATIONS) {
                    try {
                        secureRandom = SecureRandom.getInstance("NativePRNGNonBlocking");
                    } catch (Exception e) {
                        try {
                            secureRandom = SecureRandom.getInstanceStrong();
                        } catch (Exception nE) {
                            throw new RuntimeException(nE);
                        }
                    }
                    invocationCount.set(0);
                }
            }
        }
        return secureRandom;
    }

    public static PrngRandom getCurrentInstance() {
        return new PrngRandom();
    }

    public void nextBytesBlocking(byte[] destination) {
        SecureRandom secureRandom = getSecureRandom();
        secureRandom.nextBytes(destination);
    }
}
