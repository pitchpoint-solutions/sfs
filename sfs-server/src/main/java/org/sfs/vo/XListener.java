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

package org.sfs.vo;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.net.HostAndPort.fromString;

public abstract class XListener<T extends XListener> {

    private HostAndPort hostAndPort;

    public Optional<HostAndPort> getHostAndPort() {
        return fromNullable(hostAndPort);
    }

    public T setHostAndPort(HostAndPort hostAddress) {
        this.hostAndPort = hostAddress;
        return (T) this;
    }

    public abstract T copy();

    protected T copyInternal(XListener t) {
        setHostAndPort(t.hostAndPort);
        return (T) this;
    }

    public T merge(XListener<? extends XListener> other) {
        this.hostAndPort = other.hostAndPort;
        return (T) this;
    }

    public T merge(JsonObject jsonObject) {
        String v = jsonObject.getString("host_and_port");
        if (v != null) {
            this.hostAndPort = fromString(v);
        }
        return (T) this;
    }

    public JsonObject toJsonObject() {
        return new JsonObject()
                .put("host_and_port", hostAndPort.toString());
    }
}
