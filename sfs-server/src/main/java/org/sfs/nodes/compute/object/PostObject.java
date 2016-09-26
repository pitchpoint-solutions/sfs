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

package org.sfs.nodes.compute.object;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.elasticsearch.object.UpdateObject;
import org.sfs.metadata.Metadata;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionObjectUpdate;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.validate.ValidatePersistentObjectLatestVersionExists;
import org.sfs.validate.ValidateVersionNotDeleteMarker;
import org.sfs.validate.ValidateVersionNotExpired;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;

import java.util.Calendar;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.math.LongMath.checkedAdd;
import static com.google.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.primitives.Longs.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.util.Calendar.getInstance;
import static java.util.Collections.emptyList;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AFTER;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AT;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class PostObject implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(PostObject.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {
        httpServerRequest.pause();


        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateObjectPath())
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext))
                .map(new ValidatePersistentObjectLatestVersionExists())
                .flatMap(new ValidateActionObjectUpdate(httpServerRequest))
                .map(new ValidateVersionNotDeleteMarker())
                .map(new ValidateVersionNotExpired())
                .map(transientVersion -> {
                    TransientVersion newVersion = transientVersion.getParent().newVersion();
                    return newVersion.merge(transientVersion.toJsonObject());
                })
                .map(transientVersion -> {

                    Calendar updateTs = getInstance();
                    transientVersion.setUpdateTs(updateTs);

                    MultiMap headers = httpServerRequest.headers();

                    Metadata metadata = transientVersion.getMetadata();
                    metadata.clear();
                    metadata.withHttpHeaders(headers);

                    String contentEncoding = headers.get(CONTENT_ENCODING);
                    String contentType = headers.get(CONTENT_TYPE);
                    String contentDisposition = headers.get(CONTENT_DISPOSITION);
                    String deleteAt = headers.get(X_DELETE_AT);
                    String deleteAfter = headers.get(X_DELETE_AFTER);

                    if (!isNullOrEmpty(deleteAt)) {
                        Long parsed = tryParse(deleteAt);
                        transientVersion.setDeleteAt(parsed);
                    }

                    if (!isNullOrEmpty(deleteAfter)) {
                        Long parsed = tryParse(deleteAfter);
                        long now = checkedAdd(updateTs.getTimeInMillis(), parsed);
                        transientVersion.setDeleteAt(now);
                    }

                    if (isNullOrEmpty(deleteAt) && isNullOrEmpty(deleteAfter)) {
                        transientVersion.setDeleteAt(null);
                    }

                    transientVersion.setContentEncoding(contentEncoding);

                    transientVersion.setContentType(contentType);

                    transientVersion.setContentDisposition(contentDisposition);

                    return transientVersion;
                })
                .flatMap(transientVersion ->
                        just((PersistentObject) transientVersion.getParent())
                                .flatMap(new PruneObject(httpServerRequest.vertxContext(), transientVersion))
                                .map(modified -> (PersistentObject) transientVersion.getParent())
                )
                .map(persistentObject -> persistentObject.setUpdateTs(getInstance()))
                .flatMap(new UpdateObject(httpServerRequest.vertxContext()))
                .map(new ValidateOptimisticObjectLock())
                .map(persistentObject -> persistentObject.getNewestVersion().get())
                .doOnNext(version -> httpServerRequest.response().setStatusCode(HTTP_ACCEPTED))
                .flatMap(version ->
                        empty()
                                .map(new WriteHttpServerResponseHeaders(httpServerRequest, version, emptyList())))
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {


                    @Override
                    public void onNext(Void aVoid) {
                        // do nothing here since the headers are set earlier
                    }

                });

    }
}