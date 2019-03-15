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

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.HttpServerResponseEndableWriteStream;
import org.sfs.io.NoEndEndableWriteStream;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionObjectRead;
import org.sfs.validate.ValidateDynamicLargeObjectHasParts;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.validate.ValidatePersistentObjectLatestVersionExists;
import org.sfs.validate.ValidatePersistentObjectVersionExists;
import org.sfs.validate.ValidateVersionHasSegments;
import org.sfs.validate.ValidateVersionIsReadable;
import org.sfs.validate.ValidateVersionNotDeleteMarker;
import org.sfs.validate.ValidateVersionNotDeleted;
import org.sfs.validate.ValidateVersionNotExpired;
import org.sfs.validate.ValidateVersionSegmentsHasData;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.primitives.Longs.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.SfsHttpQueryParams.VERSION;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class GetObject implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(GetObject.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {
        MultiMap queryParams = httpServerRequest.params();
        final String versionAsString = queryParams.get(VERSION);

        final BufferEndableWriteStream httpResponseWriteStream = new NoEndEndableWriteStream(new HttpServerResponseEndableWriteStream(httpServerRequest.response()));

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateObjectPath())
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext))
                .flatMap(persistentObject -> {
                    if (isNullOrEmpty(versionAsString)) {
                        return just(persistentObject)
                                .map(new ValidatePersistentObjectLatestVersionExists());
                    } else {
                        final long parsedVersion = tryParse(versionAsString);
                        return just(persistentObject)
                                .map(new ValidatePersistentObjectVersionExists(parsedVersion));
                    }
                })
                .flatMap(new ValidateActionObjectRead(httpServerRequest))
                .map(new ValidateVersionNotDeleted())
                .map(new ValidateVersionNotDeleteMarker())
                .map(new ValidateVersionNotExpired())
                .map(new ValidateVersionHasSegments())
                .map(new ValidateVersionSegmentsHasData())
                .map(new ValidateVersionIsReadable())
                .flatMap(transientVersion -> {
                    // Dynamic large objects are stupid but it's the way openstack swift does
                    // there's lots of opportunity to delete dynamic large object parts
                    // or change them after the object manifest has been declared
                    // but hey... we're doing it anyways because we want to be
                    // compatible with existing gui tools that support swift.
                    // This reactor will fail if there are no parts
                    if (transientVersion.getObjectManifest().isPresent()) {
                        return just(transientVersion)
                                .flatMap(new EmitDynamicLargeObjectParts(httpServerRequest.vertxContext()))
                                .map(new ValidatePersistentObjectLatestVersionExists())
                                .flatMap(new ValidateActionObjectRead(httpServerRequest))
                                .map(new ValidateVersionNotDeleted())
                                .map(new ValidateVersionNotDeleteMarker())
                                .map(new ValidateVersionNotExpired())
                                .map(new ValidateVersionHasSegments())
                                .map(new ValidateVersionSegmentsHasData())
                                .map(new ValidateVersionIsReadable())
                                .toSortedList((lft, rgt) -> {
                                    String lftId = lft.getParent().getId();
                                    String rgtId = rgt.getParent().getId();
                                    return lftId.compareTo(rgtId);
                                })
                                .flatMap(transientVersions ->
                                        just(transientVersions)
                                                .map(new ValidateDynamicLargeObjectHasParts(transientVersion))
                                                .doOnNext(aVoid -> httpServerRequest.response().setStatusCode(HTTP_OK))
                                                .map(new WriteHttpServerResponseHeaders(httpServerRequest, transientVersion, transientVersions))
                                                .map(aVoid -> transientVersions)
                                                .flatMap(new ReadSegments(vertxContext, httpResponseWriteStream)));
                    } else {
                        return aVoid()
                                .doOnNext(aVoid -> httpServerRequest.response().setStatusCode(HTTP_OK))
                                .map(new WriteHttpServerResponseHeaders(httpServerRequest, transientVersion, emptyList()))
                                .map(aVoid -> singletonList(transientVersion))
                                .flatMap(new ReadSegments(vertxContext, httpResponseWriteStream));
                    }
                })
                .map(new ToVoid<>())
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void input) {
                        // do nothing here since the headers need to set before the stream is copied. As a result
                        // the WriteHttpServerResponseHeaders map call
                    }
                });

    }
}