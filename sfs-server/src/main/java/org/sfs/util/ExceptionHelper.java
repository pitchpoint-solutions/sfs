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
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class ExceptionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHelper.class);

    public static boolean containsException(Class<? extends Throwable> exception, Throwable e) {
        if (exception.isAssignableFrom(e.getClass())) {
            return true;
        } else {
            Throwable cause = e.getCause();
            if (cause != null) {
                return containsException(exception, cause);
            }
        }
        return false;
    }

    public static <T extends Throwable> Optional<T> unwrapCause(Class<T> exception, Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (result != null && !exception.isAssignableFrom(result.getClass())) {
            if (result.getCause() == null) {
                return Optional.absent();
            }
            if (result.getCause() == result) {
                return Optional.absent();
            }
            if (counter++ > 10) {
                LOGGER.warn("Exception cause unwrapping ran for 10 levels...", t);
                return Optional.absent();
            }
            result = result.getCause();
        }
        if (exception.isAssignableFrom(result.getClass())) {
            return Optional.of((T) result);
        } else {
            return Optional.absent();
        }
    }

    public static RuntimeException rethrow(Throwable e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new RuntimeException(e);
        }
    }
}
