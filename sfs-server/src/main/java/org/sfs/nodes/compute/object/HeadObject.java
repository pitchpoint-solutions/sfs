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
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
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
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Collections.emptyList;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.SfsHttpQueryParams.VERSION;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class HeadObject implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {
        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        MultiMap queryParams = httpServerRequest.params();
        final String versionAsString = queryParams.get(VERSION);

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
                    if (transientVersion.getObjectManifest().isPresent()) {
                        return just(transientVersion)
                                .flatMap(new EmitDynamicLargeObjectParts(httpServerRequest.vertxContext()))
                                .map(new ValidatePersistentObjectLatestVersionExists())
                                .flatMap(transientVersion1 ->
                                        just(transientVersion1)
                                                .map(new ValidateActionObjectRead(httpServerRequest))
                                                .map(aVoid -> transientVersion1))
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
                                                .map(aVoid -> transientVersions));
                    } else {
                        return aVoid()
                                .doOnNext(aVoid -> httpServerRequest.response().setStatusCode(HTTP_OK))
                                .map(new WriteHttpServerResponseHeaders(httpServerRequest, transientVersion, emptyList()));
                    }
                })
                .map(new ToVoid<>())
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {

                    @Override
                    public void onNext(Void input) {
                        // do nothing here since the headers are set earlier
                    }
                });

    }
}