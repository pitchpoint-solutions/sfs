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

package org.sfs.filesystem.volume;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.copyOf;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.valueOf;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.isDirectory;
import static org.sfs.filesystem.volume.Volume.Status.STARTED;
import static org.sfs.filesystem.volume.Volume.Status.STOPPED;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.RxHelper.iterate;
import static rx.Observable.create;
import static rx.Observable.defer;

public class VolumeManager {

    private static final Logger LOGGER = getLogger(VolumeManager.class);
    private Map<String, Volume> volumeMap = new HashMap<>();
    private Path basePath;
    private boolean open = false;

    public VolumeManager(Path basePath) {
        this.basePath = Paths.get(basePath.toString(), "volumes");
    }

    public boolean isOpen() {
        return open;
    }

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        return defer(() -> {
            open = true;

            AsyncResultMemoizeHandler<Void, Void> handler = new AsyncResultMemoizeHandler<>();
            vertxContext.vertx().fileSystem()
                    .mkdirs(basePath.toString(), null, handler);

            return create(handler.subscribe)
                    .flatMap(aVoid -> {
                        AsyncResultMemoizeHandler<List<String>, List<String>> handler1 = new AsyncResultMemoizeHandler<>();
                        vertxContext.vertx().fileSystem()
                                .readDir(basePath.toString(), handler1);
                        return create(handler1.subscribe);
                    })
                    .flatMap(Observable::from)
                    .map(volumeDirectory -> Paths.get(volumeDirectory))
                    .filter(volumeDirectory -> {
                        checkState(isDirectory(volumeDirectory), "%s must be a directory", volumeDirectory.toString());
                        return true;
                    })
                    .flatMap(volumeDirectory -> {
                        final Volume volume = new VolumeV1(volumeDirectory);
                        return volume.open(vertxContext.vertx())
                                .map(aVoid -> volume);
                    })
                    .map(volume -> {
                        volumeMap.put(volume.getVolumeId(), volume);
                        return null;
                    })
                    .count()
                    .map(new ToVoid<>())
                    .singleOrDefault(null)
                    .flatMap(o -> {
                        if (volumeMap.isEmpty()) {
                            return newVolume(vertxContext)
                                    .map(new ToVoid<>());
                        }
                        return empty();
                    });
        });
    }

    public Observable<Void> deleteVolumes(VertxContext<Server> vertxContext) {
        return empty()
                .doOnNext(aVoid -> {
                    checkState(!open, "Not closed");
                })
                .flatMap(aVoid -> {
                    AsyncResultMemoizeHandler<Void, Void> handler = new AsyncResultMemoizeHandler<>();
                    vertxContext.vertx().fileSystem().deleteRecursive(basePath.toString(), true, handler);
                    return create(handler.subscribe);
                });
    }

    public Observable<String> newVolume(VertxContext<Server> vertxContext) {
        return newVolume0(vertxContext, 0)
                .map(volume -> {
                    volumeMap.put(volume.getVolumeId(), volume);
                    return volume.getVolumeId();
                });
    }

    protected Observable<Volume> newVolume0(VertxContext<Server> vertxContext, final int offset) {
        return defer(() -> {
            final Path path = Paths.get(basePath.toString(), valueOf(volumeMap.size() + offset));
            AtomicBoolean exists = new AtomicBoolean(false);
            return vertxContext.executeBlocking(
                    () -> {
                        try {
                            createDirectory(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return (Void) null;
                    })
                    .onErrorResumeNext(throwable -> {
                        exists.set(true);
                        return empty();
                    })
                    .flatMap(aVoid -> {
                        if (!exists.get()) {
                            final Volume volume = new VolumeV1(path);
                            return volume.open(vertxContext.vertx())
                                    .map(aVoid1 -> volume);
                        } else {
                            return newVolume0(vertxContext, offset + 1);
                        }
                    });
        });

    }

    public Observable<Void> openVolume(VertxContext<Server> vertxContext, final String volumeId) {
        return empty()
                .flatMap(aVoid -> {
                    Volume volume = volumeMap.get(volumeId);
                    if (volume == null) {
                        throw new VolumeNotFoundException("Volume " + volumeId + " is not managed by this volume manager");
                    }
                    if (STOPPED.equals(volume.status())) {
                        return volume.open(vertxContext.vertx());
                    } else {
                        throw new VolumeNotStoppedException();
                    }
                });
    }

    public Observable<Void> closeVolume(VertxContext<Server> vertxContext, final String volumeId) {
        return empty()
                .flatMap(aVoid -> {
                    Volume volume = volumeMap.get(volumeId);
                    if (volume == null) {
                        throw new VolumeNotFoundException("Volume " + volumeId + " is not managed by this volume manager");
                    }
                    if (STARTED.equals(volume.status())) {
                        return volume.open(vertxContext.vertx());
                    } else {
                        throw new VolumeNotStartedException();
                    }
                });
    }

    public Iterable<String> volumes() {
        return FluentIterable.from(volumeMap.values())
                .transform(input -> input.getVolumeId());
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return defer(() -> {
            open = false;
            final ImmutableSet<Volume> values = copyOf(volumeMap.values());
            volumeMap.clear();
            return iterate(
                    values, volume -> volume.close(vertxContext.vertx())
                            .onErrorResumeNext(throwable -> {
                                LOGGER.warn("Unhandled Exception", throwable);
                                return empty();
                            })
                            .map(aVoid -> true)
            )
                    .map(new ToVoid<>());
        });
    }

    public Optional<Volume> get(String volumeId) {
        return fromNullable(volumeMap.get(volumeId));
    }

    public static class VolumeNotFoundException extends RuntimeException {

        public VolumeNotFoundException(String message) {
            super(message);
        }
    }

    public static class VolumeNotStoppedException extends RuntimeException {

    }

    public static class VolumeNotStartedException extends RuntimeException {

    }

}
