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

package org.sfs.nodes.compute.object;

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.just;

public class ReadSegments implements Func1<Iterable<TransientVersion>, Observable<Iterable<TransientVersion>>> {

    private static final Logger LOGGER = getLogger(ReadSegments.class);
    private final VertxContext<Server> vertxContext;
    private final BufferEndableWriteStream bufferStreamConsumer;
    private final boolean verifyChecksum;

    public ReadSegments(VertxContext<Server> vertxContext, BufferEndableWriteStream bufferStreamConsumer, boolean verifyChecksum) {
        this.vertxContext = vertxContext;
        this.bufferStreamConsumer = bufferStreamConsumer;
        this.verifyChecksum = verifyChecksum;
    }

    public ReadSegments(VertxContext<Server> vertxContext, BufferEndableWriteStream bufferStreamConsumer) {
        this(vertxContext, bufferStreamConsumer, false);
    }

    @Override
    public Observable<Iterable<TransientVersion>> call(Iterable<TransientVersion> transientVersions) {
        return just(transientVersions)
                .flatMap(new CopyVersionsReadStreams(vertxContext, bufferStreamConsumer, verifyChecksum));
    }

}


