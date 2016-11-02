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

import com.google.common.base.Optional;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.elasticsearch.object.LoadObject;
import org.sfs.elasticsearch.object.PersistObject;
import org.sfs.elasticsearch.object.UpdateObject;
import org.sfs.encryption.AlgorithmDef;
import org.sfs.io.CountingReadStream;
import org.sfs.io.FileBackedBuffer;
import org.sfs.io.LimitedReadStream;
import org.sfs.nodes.all.segment.AcknowledgeSegment;
import org.sfs.nodes.all.segment.VerifySegmentQuick;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.NullSubscriber;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionObjectCreate;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.validate.ValidateHeaderIsBase16LowercaseEncoded;
import org.sfs.validate.ValidateHeaderIsBase64Encoded;
import org.sfs.validate.ValidateHeaderNotExists;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.validate.ValidateParamNotExists;
import org.sfs.validate.ValidateReplicateGroupIsPresent;
import org.sfs.validate.ValidateTtl;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientObject;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.XObject;
import org.sfs.vo.XVersion;
import rx.Observable;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.ETAG;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.valueOf;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.util.Calendar.getInstance;
import static java.util.Collections.emptyList;
import static org.sfs.encryption.AlgorithmDef.getPreferred;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.Limits.MAX_SEGMENT_SIZE;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_SHA512;
import static org.sfs.util.SfsHttpHeaders.X_COPY_FROM;
import static org.sfs.util.SfsHttpQueryParams.MULTIPART_MANIFEST;
import static org.sfs.validate.HttpRequestExceptionValidations.validateContentLength;
import static org.sfs.validate.HttpRequestExceptionValidations.validateContentMd5;
import static org.sfs.validate.HttpRequestExceptionValidations.validateContentSha512;
import static org.sfs.validate.HttpRequestExceptionValidations.validateEtag;
import static org.sfs.vo.ObjectPath.fromSfsRequest;
import static rx.Observable.using;

public class PutObject implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(PutObject.class);
    private static final AlgorithmDef ALGORITHM_DEF = getPreferred();

    @Override
    public void handle(final SfsRequest httpServerRequest) {
        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        final AtomicReference<FileBackedBuffer> tempFileRef = new AtomicReference<>();

        Observable<Void> o =
                aVoid()
                        .flatMap(new Authenticate(httpServerRequest))
                        .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                        .map(aVoid -> httpServerRequest)
                        .flatMap(new ValidateReplicateGroupIsPresent(httpServerRequest.vertxContext()))
                        .map(new ValidateParamNotExists(MULTIPART_MANIFEST))
                        .map(new ValidateHeaderNotExists(X_COPY_FROM))
                        .map(new ValidateHeaderIsBase16LowercaseEncoded(ETAG))
                        .map(new ValidateHeaderIsBase64Encoded(CONTENT_MD5))
                        .map(new ValidateHeaderIsBase64Encoded(X_CONTENT_SHA512))
                        .map(new ValidateTtl())
                        .map(new ToVoid<>())
                        .flatMap(aVoid -> {
                            ObjectPath objectPath = fromSfsRequest(httpServerRequest);
                            return just(objectPath)
                                    .map(new ValidateObjectPath())
                                    .flatMap(new LoadAccountAndContainer(vertxContext))
                                    .flatMap(persistentContainer ->
                                            just(objectPath.objectPath().get())
                                                    .flatMap(new LoadObject(vertxContext, persistentContainer))
                                                    .map(oPersistentObject -> {
                                                        if (oPersistentObject.isPresent()) {
                                                            PersistentObject persistentObject = oPersistentObject.get();
                                                            return persistentObject.newVersion().merge(httpServerRequest);
                                                        } else {
                                                            final TransientObject transientObject = new TransientObject(persistentContainer, objectPath.objectPath().get())
                                                                    .setOwnerGuid(httpServerRequest.getUserAndRole().getUser().getId());
                                                            return transientObject
                                                                    .newVersion()
                                                                    .merge(httpServerRequest);
                                                        }
                                                    }));
                        })
                        .flatMap(new ValidateActionObjectCreate(httpServerRequest))
                        // do this step after we validate that we can create an object
                        // so we don't consume a stream if we won't be able to create an object
                        .flatMap(transientVersion -> {
                            final MultiMap headers = httpServerRequest.headers();
                            String contentLength = headers.get(CONTENT_LENGTH);
                            if (contentLength == null) {
                                Path tempDirectory = httpServerRequest.vertxContext().verticle().sfsFileSystem().tmpDirectory();
                                FileBackedBuffer fileBackedBuffer = new FileBackedBuffer(vertxContext.vertx(), 8192, true, tempDirectory);
                                tempFileRef.set(fileBackedBuffer);
                                LimitedReadStream readStream = new LimitedReadStream(httpServerRequest, MAX_SEGMENT_SIZE);
                                CountingReadStream countingWriteStream = new CountingReadStream(readStream);
                                return pump(countingWriteStream, fileBackedBuffer)
                                        .map(aVoid -> {
                                            transientVersion.setContentLength(countingWriteStream.count());
                                            httpServerRequest.headers().set(CONTENT_LENGTH, valueOf(countingWriteStream.count()));
                                            return transientVersion;
                                        });
                            } else {
                                return just(transientVersion);
                            }
                        })
                        .flatMap(transientVersion ->
                                just(httpServerRequest)
                                        .map(new ValidateHeaderBetweenLong(CONTENT_LENGTH, 0L, MAX_SEGMENT_SIZE))
                                        .map(httpServerRequest1 -> transientVersion))
                        .flatMap(transientVersion -> {
                            long length = transientVersion.getContentLength().get();
                            if (length > 0) {
                                return aVoid()
                                        .flatMap(aVoid -> {
                                            if (tempFileRef.get() != null) {
                                                FileBackedBuffer fileBackedBuffer = tempFileRef.get();
                                                return Observable.just(transientVersion)
                                                        .flatMap(new WriteNewSegment(httpServerRequest.vertxContext(), fileBackedBuffer.readStream()));
                                            } else {
                                                return Observable.just(transientVersion)
                                                        .flatMap(new WriteNewSegment(httpServerRequest.vertxContext(), httpServerRequest));
                                            }
                                        })
                                        .map(transientSegment -> {
                                            validateSegment(transientSegment);
                                            return transientSegment;
                                        })
                                        .map(transientSegment -> transientSegment.getParent());
                            } else {
                                return just(transientVersion);
                            }
                        })
                        .flatMap(transientVersion -> {
                            final long versionId = transientVersion.getId();
                            XObject xObject = transientVersion.getParent();
                            if (xObject instanceof PersistentObject) {
                                return just((PersistentObject) xObject)
                                        .map(persistentObject -> persistentObject.setUpdateTs(getInstance()))
                                        .flatMap(new UpdateObject(httpServerRequest.vertxContext()))
                                        .map(new ValidateOptimisticObjectLock())
                                        .map(persistentObject -> persistentObject.getVersion(versionId).get());
                            } else {
                                return just((TransientObject) xObject)
                                        .doOnNext(transientObject -> {
                                            Optional<TransientServiceDef> currentMaintainerNode =
                                                    vertxContext
                                                            .verticle()
                                                            .getClusterInfo()
                                                            .getCurrentMaintainerNode();
                                            if (currentMaintainerNode.isPresent()) {
                                                transientObject.setNodeId(currentMaintainerNode.get().getId());
                                            }
                                        })
                                        .flatMap(new PersistObject(httpServerRequest.vertxContext()))
                                        .map(new ValidateOptimisticObjectLock())
                                        .map(persistentObject -> persistentObject.getVersion(versionId).get());
                            }
                        })
                        .flatMap(transientVersion -> {
                            long length = transientVersion.getContentLength().get();
                            if (length > 0) {
                                TransientSegment latestSegment = transientVersion.getNewestSegment().get();
                                return just(latestSegment)
                                        .flatMap(new AcknowledgeSegment(httpServerRequest.vertxContext()))
                                        .map(modified -> latestSegment)
                                        .flatMap(new VerifySegmentQuick(httpServerRequest.vertxContext()))
                                        .map(verified -> {
                                            checkState(verified, "Segment verification failed");
                                            return (Void) null;
                                        })
                                        .map(aVoid -> latestSegment.getParent());
                            } else {
                                return just(transientVersion);
                            }
                        })
                        .flatMap(transientVersion -> {
                                    PersistentObject persistentObject = (PersistentObject) transientVersion.getParent();
                                    return just(persistentObject)
                                            .flatMap(new PruneObject(httpServerRequest.vertxContext(), transientVersion))
                                            .map(modified -> persistentObject.getVersion(transientVersion.getId()).get());
                                }
                        )
                        .flatMap(transientVersion -> {
                            final long versionId = transientVersion.getId();
                            XObject xObject = transientVersion.getParent();
                            return just((PersistentObject) xObject)
                                    .map(persistentObject -> persistentObject.setUpdateTs(getInstance()))
                                    .flatMap(new UpdateObject(httpServerRequest.vertxContext()))
                                    .map(new ValidateOptimisticObjectLock())
                                    .map(persistentObject -> persistentObject.getVersion(versionId).get());
                        })
                        .doOnNext(version -> httpServerRequest.response().setStatusCode(HTTP_CREATED))
                        .flatMap(version ->
                                aVoid()
                                        .map(new WriteHttpServerResponseHeaders(httpServerRequest, version, emptyList())))
                        .single();

        using(
                () -> null,
                aVoid -> o,
                aVoid -> cleanupTmp(tempFileRef)
                        .subscribe(new NullSubscriber<>()))
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                               @Override
                               public void onNext(Void aVoid) {
                                   // do nothing here since the headers are set earlier
                               }

                           }

                );

    }

    public static void validateSegment(TransientSegment transientSegment) {
        XVersion<? extends XVersion> version = transientSegment.getParent();
        Optional<byte[]> oEtag = version.getEtag();
        Optional<byte[]> oContentMd5 = version.getContentMd5();
        Optional<byte[]> oContentSha512 = version.getContentSha512();
        long contentLength = version.getContentLength().get();
        if (oEtag.isPresent()) {
            byte[] md5 = version.calculateMd5().get();
            validateEtag(oEtag.get(), md5);
        }
        if (oContentMd5.isPresent()) {
            byte[] md5 = version.calculateMd5().get();
            validateContentMd5(oContentMd5.get(), md5);
        }
        if (oContentSha512.isPresent()) {
            byte[] sha512 = version.calculateSha512().get();
            validateContentSha512(oContentSha512.get(), sha512);
        }
        validateContentLength(contentLength, version.calculateLength().get());
    }


    protected Observable<Void> cleanupTmp(AtomicReference<FileBackedBuffer> tempFileRef) {
        FileBackedBuffer fileBackedBuffer = tempFileRef.get();
        if (fileBackedBuffer != null) {
            return fileBackedBuffer.close()
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Failed to close and delete " + fileBackedBuffer, throwable);
                        return aVoid();
                    });
        }
        return aVoid();
    }
}