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

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.elasticsearch.object.LoadObject;
import org.sfs.elasticsearch.object.PersistOrUpdateVersion;
import org.sfs.elasticsearch.object.UpdateObject;
import org.sfs.encryption.Algorithm;
import org.sfs.encryption.AlgorithmDef;
import org.sfs.filesystem.JournalFile;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.InflaterEndableWriteStream;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.nodes.all.segment.AcknowledgeSegment;
import org.sfs.nodes.compute.object.WriteNewSegment;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidateContainerIsEmpty;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.validate.ValidateHeaderExists;
import org.sfs.validate.ValidateHeaderIsBase64Encoded;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.validate.ValidatePath;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientObject;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.XObject;
import rx.Observable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.InflaterOutputStream;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Splitter.on;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.primitives.Longs.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Paths.get;
import static java.util.Calendar.getInstance;
import static org.sfs.encryption.AlgorithmDef.fromNameIfExists;
import static org.sfs.filesystem.containerdump.DumpFileWriter.DUMP_FILE_NAME;
import static org.sfs.protobuf.XVolume.XDumpFile.CompressionType;
import static org.sfs.protobuf.XVolume.XDumpFile.CompressionType.DEFLATE;
import static org.sfs.protobuf.XVolume.XDumpFile.CompressionType.NONE;
import static org.sfs.protobuf.XVolume.XDumpFile.FirstHeader.parseFrom;
import static org.sfs.protobuf.XVolume.XDumpFile.Header;
import static org.sfs.protobuf.XVolume.XDumpFile.Header.Type;
import static org.sfs.protobuf.XVolume.XDumpFile.Header.Type.VERSION_01;
import static org.sfs.protobuf.XVolume.XDumpFile.Version01;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.ExceptionHelper.unwrapCause;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_SFS_IMPORT_SKIP_POSITIONS;
import static org.sfs.util.SfsHttpHeaders.X_SFS_KEEP_ALIVE_TIMEOUT;
import static org.sfs.util.SfsHttpHeaders.X_SFS_SECRET;
import static org.sfs.util.SfsHttpHeaders.X_SFS_SRC_DIRECTORY;
import static org.sfs.vo.ObjectPath.DELIMITER;
import static org.sfs.vo.ObjectPath.fromPaths;
import static org.sfs.vo.ObjectPath.fromSfsRequest;
import static rx.Observable.error;

public class ImportContainer implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(ImportContainer.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderExists(X_SFS_SRC_DIRECTORY))
                .map(new ValidateHeaderBetweenLong(X_SFS_KEEP_ALIVE_TIMEOUT, 10000, 300000))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(new LoadAccountAndContainer(vertxContext))
                .flatMap(new ValidateContainerIsEmpty(vertxContext))
                .flatMap(targetPersistentContainer -> {

                    MultiMap headers = httpServerRequest.headers();
                    String importDirectory = headers.get(X_SFS_SRC_DIRECTORY);
                    String unparsedSkipPositions = headers.get(X_SFS_IMPORT_SKIP_POSITIONS);
                    Set<Long> skipPositions;
                    if (!isNullOrEmpty(unparsedSkipPositions)) {
                        skipPositions =
                                from(on(',').trimResults().split(unparsedSkipPositions))
                                        .transform(input -> tryParse(input))
                                        .filter(notNull())
                                        .toSet();
                    } else {
                        skipPositions = new HashSet<>(0);
                    }

                    return aVoid()
                            .flatMap(aVoid -> {
                                ObservableFuture<Boolean> handler = RxHelper.observableFuture();
                                vertxContext.vertx().fileSystem().exists(importDirectory, handler.toHandler());
                                return handler
                                        .map(destDirectoryExists -> {
                                            if (!TRUE.equals(destDirectoryExists)) {
                                                JsonObject jsonObject = new JsonObject()
                                                        .put("message", format("%s does not exist", importDirectory));

                                                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                            } else {
                                                return (Void) null;
                                            }
                                        });
                            })
                            .flatMap(oVoid -> {
                                ObservableFuture<List<String>> handler = RxHelper.observableFuture();
                                vertxContext.vertx().fileSystem().readDir(importDirectory, handler.toHandler());
                                return handler
                                        .map(listing -> {
                                            if (listing.size() <= 0) {
                                                JsonObject jsonObject = new JsonObject()
                                                        .put("message", format("%s is empty", importDirectory));

                                                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                                            } else {
                                                return (Void) null;
                                            }
                                        });
                            })
                            .flatMap(aVoid -> {

                                LOGGER.info("Importing into container " + targetPersistentContainer.getId() + " from " + importDirectory);

                                JournalFile journalFile = new JournalFile(get(importDirectory).resolve(DUMP_FILE_NAME));
                                return journalFile.open(vertxContext.vertx())
                                        .map(aVoid1 -> journalFile);
                            })
                            .flatMap(journalFile -> {
                                SfsVertx sfsVertx = vertxContext.vertx();
                                return journalFile.getFirstEntry(sfsVertx)
                                        .map(entryOptional -> {
                                            checkState(entryOptional.isPresent(), "First dump file entry is corrupt");
                                            return entryOptional.get();
                                        })
                                        .flatMap(entry ->
                                                entry.getMetadata(sfsVertx)
                                                        .map(buffer -> {
                                                            try {
                                                                return parseFrom(buffer.getBytes());
                                                            } catch (InvalidProtocolBufferException e) {
                                                                throw new RuntimeException(e);
                                                            }
                                                        })
                                                        .flatMap(firstHeader -> {
                                                            if (firstHeader.getEncrypted()) {
                                                                return just(httpServerRequest)
                                                                        .map(new ValidateHeaderExists(X_SFS_SECRET))
                                                                        .map(new ValidateHeaderIsBase64Encoded(X_SFS_SECRET))
                                                                        .map(new ToVoid<>())
                                                                        .map(aVoid -> {
                                                                            String cipherName = firstHeader.getCipherName();
                                                                            checkState(!isNullOrEmpty(cipherName), "Encryption is enabled by cipher name is not specified");
                                                                            AlgorithmDef algorithmDef = fromNameIfExists(cipherName);
                                                                            checkState(algorithmDef != null, "Algorithm %s not found", cipherName);
                                                                            return new ImportStartState(
                                                                                    journalFile,
                                                                                    entry.getNextHeaderPosition(),
                                                                                    algorithmDef,
                                                                                    base64().decode(headers.get(X_SFS_SECRET)));
                                                                        });
                                                            } else {
                                                                return just(new ImportStartState(
                                                                        journalFile,
                                                                        entry.getNextHeaderPosition(),
                                                                        null,
                                                                        null));
                                                            }
                                                        }));
                            })
                            .flatMap(importStartState -> {

                                JournalFile journalFile = importStartState.getJournalFile();

                                long startPosition = importStartState.getStartPosition();
                                boolean encrypted = importStartState.getAlgorithmDef() != null;
                                byte[] secret = importStartState.getSecret();
                                AlgorithmDef algorithmDef = importStartState.getAlgorithmDef();

                                httpServerRequest.startProxyKeepAlive();
                                SfsVertx sfsVertx = vertxContext.vertx();

                                return journalFile.scan(sfsVertx, startPosition, entry -> {
                                    // skip over any positions that should be skipped
                                    if (skipPositions.contains(entry.getHeaderPosition())) {
                                        return just(true);
                                    }
                                    return entry.getMetadata(sfsVertx)
                                            .flatMap(buffer -> {
                                                try {
                                                    Header header = Header.parseFrom(buffer.getBytes());
                                                    Type type = header.getType();
                                                    checkState(VERSION_01.equals(type), "Type was %s, expected %s", type, VERSION_01);

                                                    byte[] cipherDataSalt = header.getCipherDataSalt() != null ? header.getCipherDataSalt().toByteArray() : null;
                                                    byte[] cipherMetadataSalt = header.getCipherMetadataSalt() != null ? header.getCipherMetadataSalt().toByteArray() : null;

                                                    CompressionType metadataCompressionType = header.getMetadataCompressionType();
                                                    checkState(NONE.equals(metadataCompressionType) || DEFLATE.equals(metadataCompressionType), "Metadata compression type was %s, expected %s", metadataCompressionType, DEFLATE);

                                                    CompressionType dataCompressionType = header.getDataCompressionType();
                                                    checkState(NONE.equals(dataCompressionType) || DEFLATE.equals(dataCompressionType), "Data compression type was %s, expected %s", dataCompressionType, DEFLATE);


                                                    byte[] marshaledExportObject = header.getData().toByteArray();

                                                    if (encrypted) {
                                                        checkState(cipherMetadataSalt != null && cipherMetadataSalt.length > 0);
                                                        Algorithm algorithm = algorithmDef.create(secret, cipherMetadataSalt);
                                                        marshaledExportObject = algorithm.decrypt(marshaledExportObject);
                                                    }

                                                    if (DEFLATE.equals(metadataCompressionType)) {
                                                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                                                        try (InflaterOutputStream inflaterOutputStream = new InflaterOutputStream(byteArrayOutputStream)) {
                                                            inflaterOutputStream.write(marshaledExportObject);
                                                        } catch (IOException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                        marshaledExportObject = byteArrayOutputStream.toByteArray();
                                                    }

                                                    Version01 exportObject = Version01.parseFrom(marshaledExportObject);
                                                    ObjectPath originalObjectPath = fromPaths(exportObject.getObjectId());
                                                    String originalAccountName = originalObjectPath.accountName().get();
                                                    String originalContainerName = originalObjectPath.containerName().get();
                                                    String originalObjectName = originalObjectPath.objectName().get();
                                                    ObjectPath targetObjectPath = fromPaths(targetPersistentContainer.getId(), originalObjectName);
                                                    ValidatePath validatePath = new ValidateObjectPath();
                                                    validatePath.call(targetObjectPath);
                                                    String targetObjectId = targetObjectPath.objectPath().get();
                                                    String targetAccountName = targetObjectPath.accountName().get();
                                                    String targetContainerName = targetObjectPath.containerName().get();

                                                    return just(targetObjectId)
                                                            .flatMap(new LoadObject(vertxContext, targetPersistentContainer))
                                                            .map(oPersistentObject -> {
                                                                if (oPersistentObject.isPresent()) {
                                                                    PersistentObject persistentObject = oPersistentObject.get();
                                                                    return persistentObject.newVersion().merge(exportObject);
                                                                } else {
                                                                    final TransientObject transientObject = new TransientObject(targetPersistentContainer, targetObjectId)
                                                                            .setOwnerGuid(exportObject.getOwnerGuid());
                                                                    return transientObject
                                                                            .newVersion()
                                                                            .merge(exportObject);
                                                                }
                                                            })
                                                            .flatMap(transientVersion -> {
                                                                long length = transientVersion.getContentLength().get();
                                                                if (length > 0 && !transientVersion.isDeleted()) {
                                                                    return aVoid()
                                                                            .flatMap(aVoid -> {

                                                                                PipedReadStream pipedReadStream = new PipedReadStream();
                                                                                BufferEndableWriteStream bufferStreamConsumer = new PipedEndableWriteStream(pipedReadStream);
                                                                                if (DEFLATE.equals(dataCompressionType)) {
                                                                                    bufferStreamConsumer = new InflaterEndableWriteStream(bufferStreamConsumer);
                                                                                }
                                                                                if (encrypted) {
                                                                                    checkState(cipherDataSalt != null && cipherDataSalt.length > 0);
                                                                                    Algorithm algorithm = algorithmDef.create(secret, cipherDataSalt);
                                                                                    bufferStreamConsumer = algorithm.decrypt(bufferStreamConsumer);
                                                                                }
                                                                                Observable<Void> oProducer = entry.produceData(sfsVertx, bufferStreamConsumer);
                                                                                Observable<TransientSegment> oConsumer =
                                                                                        just(transientVersion)
                                                                                                .flatMap(new WriteNewSegment(vertxContext, pipedReadStream));
                                                                                return combineSinglesDelayError(oProducer, oConsumer, (aVoid1, transientSegment) -> transientSegment);
                                                                            })
                                                                            .map(transientSegment -> transientSegment.getParent());
                                                                } else {
                                                                    return just(transientVersion);
                                                                }
                                                            })
                                                            .doOnNext(transientVersion -> {
                                                                Optional<String> oObjectManifest = transientVersion.getObjectManifest();
                                                                if (oObjectManifest.isPresent()) {
                                                                    String objectManifest = oObjectManifest.get();
                                                                    int indexOfObjectName = objectManifest.indexOf(DELIMITER);
                                                                    if (indexOfObjectName > 0) {
                                                                        String containerName = objectManifest.substring(0, indexOfObjectName);
                                                                        // only adjust the object manifest if the manifest references objects
                                                                        // in the container that was exported
                                                                        if (Objects.equals(containerName, originalContainerName)) {
                                                                            objectManifest = targetContainerName + DELIMITER + objectManifest.substring(indexOfObjectName + 1);
                                                                            transientVersion.setObjectManifest(objectManifest);
                                                                        }
                                                                    }
                                                                }
                                                            })
                                                            .flatMap(new PersistOrUpdateVersion(vertxContext))
                                                            .flatMap(transientVersion -> {
                                                                long length = transientVersion.getContentLength().get();
                                                                if (length > 0 && !transientVersion.getSegments().isEmpty()) {
                                                                    TransientSegment latestSegment = transientVersion.getNewestSegment().get();
                                                                    return just(latestSegment)
                                                                            .flatMap(new AcknowledgeSegment(httpServerRequest.vertxContext()))
                                                                            .map(modified -> latestSegment.getParent());
                                                                } else {
                                                                    return just(transientVersion);
                                                                }
                                                            })
                                                            .flatMap(transientVersion -> {
                                                                final long versionId = transientVersion.getId();
                                                                XObject xObject = transientVersion.getParent();
                                                                return just((PersistentObject) xObject)
                                                                        .map(persistentObject -> persistentObject.setUpdateTs(getInstance()))
                                                                        .flatMap(new UpdateObject(httpServerRequest.vertxContext()))
                                                                        .map(new ValidateOptimisticObjectLock())
                                                                        .map(persistentObject -> persistentObject.getVersion(versionId).get());
                                                            })
                                                            .map(version -> TRUE);
                                                } catch (InvalidProtocolBufferException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            })
                                            .onErrorResumeNext(throwable -> error(new IgnorePositionRuntimeException(throwable, entry.getHeaderPosition())));
                                });
                            })
                            .doOnNext(aVoid -> LOGGER.info("Done importing into container " + targetPersistentContainer.getId() + " from " + importDirectory))
                            .map(new ToVoid<>())
                            .map(aVoid -> {
                                JsonObject jsonResponse = new JsonObject();
                                jsonResponse.put("code", HTTP_OK);
                                return jsonResponse;
                            })
                            .onErrorResumeNext(throwable -> {
                                LOGGER.info("Failed importing into container " + targetPersistentContainer.getId() + " from " + importDirectory, throwable);
                                Optional<IgnorePositionRuntimeException> oIgnorePosition = unwrapCause(IgnorePositionRuntimeException.class, throwable);
                                if (oIgnorePosition.isPresent()) {
                                    IgnorePositionRuntimeException ignorePositionRuntimeException = oIgnorePosition.get();
                                    LOGGER.error("Handling Exception", ignorePositionRuntimeException);
                                    long positionToIgnore = ignorePositionRuntimeException.getPosition();
                                    JsonObject jsonResponse = new JsonObject();
                                    skipPositions.add(positionToIgnore);
                                    String joined = Joiner.on(',').join(skipPositions);
                                    jsonResponse.put("code", HTTP_INTERNAL_ERROR);
                                    jsonResponse.put("message", format("If you would like to ignore this position set the %s header with the value %s", X_SFS_IMPORT_SKIP_POSITIONS, joined));
                                    jsonResponse.put(X_SFS_IMPORT_SKIP_POSITIONS, joined);
                                    return just(jsonResponse);
                                } else {
                                    return error(throwable);
                                }
                            });

                })
                .single()
                .subscribe(new ConnectionCloseTerminus<JsonObject>(httpServerRequest) {
                    @Override
                    public void onNext(JsonObject jsonResponse) {
                        HttpServerResponse httpResponse = httpServerRequest.response();
                        httpResponse.write(jsonResponse.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }

    private static class IgnorePositionRuntimeException extends RuntimeException {

        private final long position;

        public IgnorePositionRuntimeException(Throwable cause, long position) {
            super(cause);
            this.position = position;
        }

        public long getPosition() {
            return position;
        }
    }

    private static class ImportStartState {
        private final JournalFile journalFile;
        private final long startPosition;
        private final AlgorithmDef algorithmDef;
        private final byte[] secret;

        public ImportStartState(JournalFile journalFile, long startPosition, AlgorithmDef algorithmDef, byte[] secret) {
            this.journalFile = journalFile;
            this.startPosition = startPosition;
            this.algorithmDef = algorithmDef;
            this.secret = secret;
        }

        public JournalFile getJournalFile() {
            return journalFile;
        }

        public long getStartPosition() {
            return startPosition;
        }

        public AlgorithmDef getAlgorithmDef() {
            return algorithmDef;
        }

        public byte[] getSecret() {
            return secret;
        }
    }
}

