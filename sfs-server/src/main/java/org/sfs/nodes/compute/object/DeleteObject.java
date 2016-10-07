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
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.elasticsearch.object.RemoveObject;
import org.sfs.elasticsearch.object.UpdateObject;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionObjectDelete;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Splitter.on;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.primitives.Longs.tryParse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.util.Calendar.getInstance;
import static java.util.Collections.emptySet;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpQueryParams.VERSION;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class DeleteObject implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(DeleteObject.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {


        MultiMap queryParams = httpServerRequest.params();
        final String versionAsString = queryParams.get(VERSION);

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateObjectPath())
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext))
                .flatMap(persistentObject -> {
                    int maxRevisions = persistentObject.getParent().getMaxObjectRevisions();
                    boolean hasVersionString = !isNullOrEmpty(versionAsString);
                    // if there is no version number supplied
                    // add a delete marker if the newest version
                    // isn't already a delete marker
                    List<TransientVersion> versionsToCheck = new ArrayList<>();
                    TransientVersion newVersion = null;
                    if (!hasVersionString) {
                        if (maxRevisions <= 0) {
                            for (TransientVersion transientVersion : persistentObject.getVersions()) {
                                transientVersion.setDeleted(TRUE);
                                versionsToCheck.add(transientVersion);
                            }
                        } else {
                            Optional<TransientVersion> oNewestVersion = persistentObject.getNewestVersion();
                            if (oNewestVersion.isPresent()) {
                                TransientVersion newestVersion = oNewestVersion.get();
                                versionsToCheck.add(newestVersion);
                                if (!TRUE.equals(newestVersion.getDeleteMarker())) {
                                    newVersion = persistentObject.newVersion()
                                            .merge(newestVersion.toJsonObject())
                                            .setDeleteMarker(TRUE);
                                }
                            }
                        }
                    } else {
                        final boolean all = equalsIgnoreCase("all", versionAsString);
                        final Set<Long> toDeleteVersions;
                        if (!all) {
                            toDeleteVersions =
                                    from(on(',').omitEmptyStrings().trimResults().split(versionAsString))
                                            .transform(input -> tryParse(input))
                                            .filter(notNull())
                                            .toSet();
                        } else {
                            toDeleteVersions = emptySet();
                        }

                        Iterable<TransientVersion> versionsToDelete =
                                from(persistentObject.getVersions())
                                        .filter(notNull())
                                        .filter(input -> all || toDeleteVersions.contains(input.getId()));

                        for (TransientVersion version : versionsToDelete) {
                            version.setDeleted(TRUE);
                            versionsToCheck.add(version);
                        }
                    }

                    TransientVersion finalNewVersion = newVersion;
                    return Observable.from(versionsToCheck)
                            .flatMap(new ValidateActionObjectDelete(httpServerRequest))
                            .count()
                            .map(new ToVoid<>())
                            .flatMap(aVoid -> {
                                if (finalNewVersion != null) {
                                    return new PruneObject(httpServerRequest.vertxContext(), finalNewVersion).call(persistentObject)
                                            .map(modified -> persistentObject);
                                } else {
                                    return new PruneObject(httpServerRequest.vertxContext()).call(persistentObject)
                                            .map(modified -> persistentObject);
                                }
                            });
                })
                .flatMap(persistentObject -> {
                    if (persistentObject.getVersions().isEmpty()) {
                        return just(persistentObject)
                                .flatMap(new RemoveObject(httpServerRequest.vertxContext()))
                                .map(new ValidateOptimisticObjectLock());
                    } else {
                        return just(persistentObject)
                                .map(persistentObject1 -> persistentObject.setUpdateTs(getInstance()))
                                .flatMap(new UpdateObject(httpServerRequest.vertxContext()))
                                .map(new ValidateOptimisticObjectLock());
                    }
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<PersistentObject>(httpServerRequest) {
                    @Override
                    public void onNext(PersistentObject input) {
                        httpServerRequest.response().setStatusCode(HTTP_NO_CONTENT);
                    }
                });

    }
}