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

package org.sfs.nodes.compute.container;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.ScanAndScrollStreamProducer;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.filesystem.JournalFile;
import org.sfs.filesystem.containerdump.DumpFileWriter;
import org.sfs.jobs.VerifyRepairAllContainerObjects;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.validate.ValidateHeaderExists;
import org.sfs.validate.ValidateHeaderIsBase64Encoded;
import org.sfs.validate.ValidateHeaderIsBoolean;
import rx.Observable;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.BaseEncoding.base64;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Files.write;
import static java.nio.file.Paths.get;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.filesystem.containerdump.DumpFileWriter.DUMP_FILE_NAME;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_SFS_COMPRESS;
import static org.sfs.util.SfsHttpHeaders.X_SFS_DEST_DIRECTORY;
import static org.sfs.util.SfsHttpHeaders.X_SFS_KEEP_ALIVE_TIMEOUT;
import static org.sfs.util.SfsHttpHeaders.X_SFS_SECRET;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class ExportContainer implements Handler<SfsRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportContainer.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
        SfsVertx sfsVertx = vertxContext.vertx();

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderExists(X_SFS_DEST_DIRECTORY))
                .map(new ValidateHeaderIsBoolean(X_SFS_COMPRESS))
                .map(new ValidateHeaderIsBase64Encoded(X_SFS_SECRET))
                .map(new ValidateHeaderBetweenLong(X_SFS_KEEP_ALIVE_TIMEOUT, 10000, 300000))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(new LoadAccountAndContainer(vertxContext))
                .flatMap(persistentContainer -> {

                    MultiMap headers = httpServerRequest.headers();
                    String destDirectory = headers.get(X_SFS_DEST_DIRECTORY);
                    boolean compress = "true".equalsIgnoreCase(headers.get(X_SFS_COMPRESS));
                    byte[] secret = headers.contains(X_SFS_SECRET) ? base64().decode(headers.get(X_SFS_SECRET)) : null;

                    return aVoid()
                            .flatMap(aVoid -> {
                                ObservableFuture<Boolean> handler = RxHelper.observableFuture();
                                vertxContext.vertx().fileSystem().exists(destDirectory, handler.toHandler());
                                return handler
                                        .map(destDirectoryExists -> {
                                            if (!TRUE.equals(destDirectoryExists)) {
                                                JsonObject jsonObject = new JsonObject()
                                                        .put("message", format("%s does not exist", destDirectory));

                                                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                            } else {
                                                return (Void) null;
                                            }
                                        });
                            })
                            .flatMap(oVoid -> {
                                ObservableFuture<List<String>> handler = RxHelper.observableFuture();
                                vertxContext.vertx().fileSystem().readDir(destDirectory, handler.toHandler());
                                return handler
                                        .map(listing -> {
                                            if (listing.size() > 0) {
                                                JsonObject jsonObject = new JsonObject()
                                                        .put("message", format("%s is not empty", destDirectory));

                                                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                            } else {
                                                return (Void) null;
                                            }
                                        });
                            })
                            .flatMap(aVoid -> {

                                LOGGER.info("Exporting container " + persistentContainer.getId() + " to " + destDirectory);

                                JournalFile dumpFile = new JournalFile(get(destDirectory).resolve(DUMP_FILE_NAME));
                                return dumpFile.open(vertxContext.vertx())
                                        .flatMap(aVoid1 -> dumpFile.enableWrites(vertxContext.vertx()))
                                        .map(aVoid1 -> dumpFile);
                            })
                            .flatMap(dumpFile -> {

                                httpServerRequest.startProxyKeepAlive();

                                Elasticsearch elasticsearch = vertxContext.verticle().elasticsearch();
                                String containerId = persistentContainer.getId();
                                String objectIndex = elasticsearch.objectIndex(persistentContainer.getName());

                                long now = System.currentTimeMillis() - VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD;
                                Calendar consistencyThreshold = Calendar.getInstance();
                                consistencyThreshold.setTimeInMillis(now);

                                TermQueryBuilder containerIdQuery = termQuery("container_id", containerId);

                                ScanAndScrollStreamProducer producer =
                                        new ScanAndScrollStreamProducer(vertxContext, containerIdQuery)
                                                .setIndeces(objectIndex)
                                                .setTypes(elasticsearch.defaultType())
                                                .setReturnVersion(true);

                                DumpFileWriter fileWriter = new DumpFileWriter(vertxContext, persistentContainer, dumpFile);
                                if (compress) {
                                    fileWriter.enableDataCompression();
                                }
                                if (secret != null) {
                                    fileWriter.enableDataEncryption(secret);
                                }

                                return pump(producer, fileWriter)
                                        .map(aVoid -> dumpFile);
                            })
                            .flatMap(journalFile -> journalFile.disableWrites(vertxContext.vertx())
                                    .map(aVoid -> journalFile))
                            .flatMap(journalFile -> journalFile.force(vertxContext.vertx(), true)
                                    .map(aVoid -> journalFile))
                            .flatMap(journalFile -> journalFile.close(vertxContext.vertx())
                                    .map(aVoid -> journalFile))
                            .flatMap(journalFile ->
                                    {
                                        Context context = vertxContext.vertx().getOrCreateContext();
                                        return RxHelper.executeBlocking(context, sfsVertx.getBackgroundPool(), () -> {
                                            try {
                                                write(get(destDirectory).resolve(".successful"), new byte[0], CREATE_NEW, WRITE);
                                                return (Void) null;
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        });
                                    }
                            )
                            .doOnNext(aVoid -> LOGGER.info("Done exporting container " + persistentContainer.getId() + " to " + destDirectory))
                            .onErrorResumeNext(throwable -> {
                                LOGGER.info("Failed exporting container " + persistentContainer.getId() + " to " + destDirectory, throwable);
                                return Observable.error(throwable);
                            });

                })
                .map(new ToVoid<>())
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {
                        JsonObject jsonResponse = new JsonObject();
                        jsonResponse.put("code", HTTP_OK);
                        HttpServerResponse httpResponse = httpServerRequest.response();
                        httpResponse.write(jsonResponse.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }
}
