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

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.iterate;

public class CopyVersionsReadStreams implements Func1<Iterable<TransientVersion>, Observable<Iterable<TransientVersion>>> {

    private static final Logger LOGGER = getLogger(CopyVersionsReadStreams.class);
    private final VertxContext<Server> vertxContext;
    private final BufferEndableWriteStream writeStream;
    private final boolean verifyChecksum;

    public CopyVersionsReadStreams(VertxContext<Server> vertxContext, BufferEndableWriteStream writeStream, boolean verifyChecksum) {
        this.vertxContext = vertxContext;
        this.writeStream = writeStream;
        this.verifyChecksum = verifyChecksum;
    }

    @Override
    public Observable<Iterable<TransientVersion>> call(Iterable<TransientVersion> transientVersions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin copy segment version streams");
        }
        Vertx vertx = vertxContext.vertx();
        return iterate(
                vertx,
                transientVersions, transientVersion -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("begin copy version object=" + transientVersion.getParent().getId() + ", version=" + transientVersion.getId());
                    }
                    return just(transientVersion.getSegments())
                            .flatMap(new CopySegmentsReadStreams(vertxContext, writeStream, verifyChecksum))
                            .map(transientSegments -> {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("end copy version object=" + transientVersion.getParent().getId() + ", version=" + transientVersion.getId());
                                }
                                return true;
                            });
                }
        )
                .map(_continue -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end copy segment version streams");
                    }
                    return transientVersions;
                });
    }
}

