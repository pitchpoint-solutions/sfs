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

package org.sfs;

import com.google.common.net.HostAndPort;
import io.vertx.core.Vertx;

import java.util.Objects;

public class HttpClientKey {
    private final Vertx vertx;
    private final HostAndPort hostAndPort;

    public HttpClientKey(SfsVertx vertx, HostAndPort hostAndPort) {
        this.vertx = vertx;
        this.hostAndPort = hostAndPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HttpClientKey)) return false;
        HttpClientKey that = (HttpClientKey) o;
        return Objects.equals(vertx, that.vertx) &&
                Objects.equals(hostAndPort, that.hostAndPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertx, hostAndPort);
    }
}
