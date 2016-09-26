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

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.all.blobreference.DeleteBlobReference;
import org.sfs.rx.Holder1;
import org.sfs.rx.ToVoid;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.XVersion;
import rx.Observable;
import rx.functions.Func1;

import java.util.Set;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.util.Collections.emptySet;
import static org.sfs.rx.Defer.just;
import static rx.Observable.from;

public class DeleteDataInVolumes implements Func1<PersistentObject, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(DeleteDataInVolumes.class);
    private final VertxContext<Server> vertxContext;
    private final Set<XVersion> excludes;

    public DeleteDataInVolumes(VertxContext<Server> vertxContext, Set<XVersion> excludes) {
        this.vertxContext = vertxContext;
        this.excludes = excludes;
    }

    public DeleteDataInVolumes(VertxContext<Server> vertxContext) {
        this(vertxContext, emptySet());
    }

    @Override
    public Observable<Boolean> call(final PersistentObject persistentObject) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin deletedatainvolumes object=" + persistentObject.getId());
        }
        Holder1<Boolean> modifiedHolder = new Holder1<>(false);
        return just(persistentObject)
                .flatMap(persistentObject1 -> from(persistentObject1.getVersions()))
                .filter(version -> !excludes.contains(version))
                .filter(XVersion::isDeleted)
                .flatMap(transientVersion -> from(transientVersion.getSegments()))
                .doOnNext(transientSegment -> {
                    if (transientSegment.isTinyData()) {
                        transientSegment.deleteTinyData();
                    }
                })
                .filter(transientSegment -> !transientSegment.isTinyData())
                .flatMap(transientSegment -> from(transientSegment.getBlobs()))
                .flatMap(transientBlobReference ->
                        just(transientBlobReference)
                                .flatMap(new DeleteBlobReference(vertxContext))
                                .filter(deleted -> deleted)
                                .map(deleted -> {
                                    transientBlobReference.setDeleted(deleted);
                                    if (deleted && LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("mark deletedatainvolumes object=" + persistentObject.getId());
                                    }
                                    modifiedHolder.value |= true;
                                    return (Void) null;
                                }))
                .count()
                .map(new ToVoid<>())
                .singleOrDefault(null)
                .map(aVoid -> modifiedHolder.value)
                .map(modified -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end deletedatainvolumes object=" + persistentObject.getId());
                    }
                    return modified;
                });

    }
}
