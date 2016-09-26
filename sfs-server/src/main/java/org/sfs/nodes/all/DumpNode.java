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

package org.sfs.nodes.all;


import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.ListSfsStorageIndexes;
import org.sfs.elasticsearch.documents.DumpDocumentsForNode;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.io.AsyncFileEndableWriteStream;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.Terminus;
import org.sfs.rx.ToType;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateHeaderExists;
import org.sfs.validate.ValidateNodeIsDataNode;
import rx.Observable;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Paths.get;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_SFS_COPY_DEST_DIRECTORY;
import static org.sfs.util.SfsHttpHeaders.X_SFS_KEEP_ALIVE_TIMEOUT;
import static rx.Observable.create;
import static rx.Observable.from;

public class DumpNode implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(DumpNode.class);

    @Override
    public void handle(SfsRequest httpServerRequest) {

        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderExists(X_SFS_COPY_DEST_DIRECTORY))
                .map(new ValidateNodeIsDataNode<>(vertxContext))
                .flatMap(sfsRequest -> {
                    MultiMap headers = httpServerRequest.headers();
                    String destDirectory = headers.get(X_SFS_COPY_DEST_DIRECTORY);

                    AsyncResultMemoizeHandler<Boolean, Boolean> handler = new AsyncResultMemoizeHandler<>();
                    vertxContext.vertx().fileSystem().exists(destDirectory, handler);
                    return create(handler.subscribe)
                            .map(destDirectoryExists -> {
                                if (!TRUE.equals(destDirectoryExists)) {
                                    JsonObject jsonObject = new JsonObject()
                                            .put("message", format("%s does not exist", destDirectory));

                                    throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                } else {
                                    return sfsRequest;
                                }
                            });
                })
                .flatMap(sfsRequest -> {
                    MultiMap headers = httpServerRequest.headers();
                    String destDirectory = headers.get(X_SFS_COPY_DEST_DIRECTORY);

                    AsyncResultMemoizeHandler<List<String>, List<String>> handler = new AsyncResultMemoizeHandler<>();
                    vertxContext.vertx().fileSystem().readDir(destDirectory, handler);
                    return create(handler.subscribe)
                            .map(listing -> {
                                if (listing.size() > 0) {
                                    JsonObject jsonObject = new JsonObject()
                                            .put("message", format("%s is not empty", destDirectory));

                                    throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                } else {
                                    return sfsRequest;
                                }
                            });
                })
                .map(new ToType<>((Void) null))
                .flatMap(aVoid -> {

                    MultiMap headers = httpServerRequest.headers();

                    String destDirectory = headers.get(X_SFS_COPY_DEST_DIRECTORY);

                    final long keepAliveTimeout = headers.contains(X_SFS_KEEP_ALIVE_TIMEOUT) ? parseLong(headers.get(X_SFS_KEEP_ALIVE_TIMEOUT)) : SECONDS.toMillis(10);

                    // let the client know we're alive by sending pings on the response stream
                    httpServerRequest.startProxyKeepAlive(keepAliveTimeout, MILLISECONDS);

                    String nodeId = vertxContext.verticle().nodes().getNodeId();

                    Path indexPath = get(destDirectory, "index");
                    Path volumePath = get(destDirectory, "volumes");

                    return combineSinglesDelayError(dumpIndex(vertxContext, indexPath, nodeId), dumpVolumes(vertxContext, volumePath), (aVoid1, aVoid2) -> (Void) null);

                })
                .flatMap(aVoid -> httpServerRequest.stopKeepAlive())
                .onErrorResumeNext(throwable ->
                        httpServerRequest.stopKeepAlive()
                                .flatMap(aVoid -> Observable.<Void>error(throwable)))
                .single()
                .subscribe(new Terminus<Void>(httpServerRequest) {

                    @Override
                    public void onNext(Void aVoid) {
                        JsonObject responseJson = new JsonObject()
                                .put("code", HTTP_OK)
                                .put("message", "Success");
                        httpServerRequest.response()
                                .write(responseJson.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }

    protected Observable<Void> dumpIndex(VertxContext<Server> vertxContext, Path destDirectory, String nodeId) {
        return empty()
                .flatMap(new ListSfsStorageIndexes(vertxContext))
                .flatMap(index -> {
                    Path dataFile = get(destDirectory.toString(), index + ".jsonl");
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Starting dump of index to " + dataFile.toString());
                    }
                    FileSystem fs = vertxContext.vertx().fileSystem();
                    AsyncResultMemoizeHandler<Void, Void> mkDirHandler = new AsyncResultMemoizeHandler<>();
                    fs.mkdirs(destDirectory.toString(), null, mkDirHandler);
                    return create(mkDirHandler.subscribe)
                            .flatMap(aVoid -> {
                                AsyncResultMemoizeHandler<AsyncFile, AsyncFile> handler = new AsyncResultMemoizeHandler<>();
                                OpenOptions openOptions = new OpenOptions();
                                openOptions.setCreate(true)
                                        .setRead(true)
                                        .setWrite(true);
                                vertxContext.vertx().fileSystem().open(dataFile.toString(), openOptions, handler);
                                return create(handler.subscribe)
                                        .map(AsyncFileEndableWriteStream::new)
                                        .flatMap(writeStream ->
                                                empty()
                                                        .flatMap(new DumpDocumentsForNode(vertxContext, nodeId, index, writeStream)));
                            })
                            .map(aVoid -> {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Finished dump of index to " + dataFile.toString());
                                }
                                return (Void) null;
                            });
                })
                .count()
                .map(new ToVoid<>())
                .singleOrDefault(null);

    }

    protected Observable<Void> dumpVolumes(VertxContext<Server> vertxContext, Path destDirectory) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting dump of volumes to " + destDirectory.toString());
        }
        VolumeManager volumeManager = vertxContext.verticle().nodes().volumeManager();
        return from(volumeManager.getPrimary())
                .flatMap(volume -> volume.copy(vertxContext.vertx(), get(destDirectory.toString(), volume.getVolumeId())))
                .ignoreElements()
                .singleOrDefault(null)
                .map(aVoid -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Finished dump of volumes to " + destDirectory.toString());
                    }
                    return null;
                });
    }
}

