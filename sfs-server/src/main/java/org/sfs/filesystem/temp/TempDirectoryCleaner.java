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

package org.sfs.filesystem.temp;

import io.vertx.core.Context;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.deleteIfExists;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.rx.Defer.aVoid;
import static rx.Observable.defer;

public class TempDirectoryCleaner {

    private static final Logger LOGGER = getLogger(TempDirectoryCleaner.class);
    private Set<Long> periodics = new HashSet<>();
    private Long ttl;
    private VertxContext<Server> vertxContext;
    private ExecutorService ioPool;

    public TempDirectoryCleaner() {
    }

    public Observable<Void> start(VertxContext<Server> vertxContext, long ttl) {
        this.ttl = ttl;
        this.vertxContext = vertxContext;
        this.ioPool = vertxContext.getIoPool();
        return defer(() -> {
            ObservableFuture<Void> handler = RxHelper.observableFuture();
            vertxContext.vertx()
                    .fileSystem()
                    .mkdirs(vertxContext.verticle().sfsFileSystem().tmpDirectory().toString(), null, handler.toHandler());
            long id = vertxContext.vertx()
                    .setPeriodic(SECONDS.toMillis(1),
                            event -> RxHelper.executeBlocking(vertxContext.vertx().getOrCreateContext(), ioPool, () -> {
                                deleteExpired();
                                return (Void) null;
                            })
                                    .single()
                                    .subscribe(
                                            aVoid -> {

                                            },
                                            throwable -> LOGGER.warn("Unhandled Exception", throwable),
                                            () -> {

                                            }));
            periodics.add(id);
            return handler;
        });
    }

    public Observable<Void> stop() {
        return defer(() -> {
            for (Long periodic : periodics) {
                vertxContext.vertx()
                        .cancelTimer(periodic);
            }
            periodics.clear();
            return aVoid();
        });
    }

    public Long getTtl() {
        return ttl;
    }

    protected void deleteExpired() {
        for (File expired : listExpired()) {
            try {
                deleteIfExists(expired.toPath());
            } catch (Throwable e) {
                LOGGER.warn("Failed to delete " + expired.toPath().toString(), e);
            }
        }
    }

    protected File[] listExpired() {
        long now = currentTimeMillis();
        File tmpFolder = vertxContext.verticle().sfsFileSystem().tmpDirectory().toFile();
        File[] expired =
                tmpFolder.listFiles(pathname -> now - pathname.lastModified() >= ttl);
        return expired;
    }
}
