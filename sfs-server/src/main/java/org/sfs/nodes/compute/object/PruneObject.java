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

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.all.versions.ExpireVersions;
import org.sfs.rx.Holder1;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.XVersion;
import rx.Observable;
import rx.functions.Func1;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.just;

public class PruneObject implements Func1<PersistentObject, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(PruneObject.class);
    private final VertxContext<Server> vertxContext;
    private final Set<XVersion> excludes;

    public PruneObject(VertxContext<Server> vertxContext, XVersion... excludes) {
        this.vertxContext = vertxContext;
        this.excludes = newHashSet(excludes);
    }

    @Override
    public Observable<Boolean> call(PersistentObject persistentObject) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin prune object=" + persistentObject.getId());
        }
        Holder1<Boolean> modifiedAck = new Holder1<>(false);
        return just(persistentObject)
                .map(new ExpireVersions(excludes))
                .map(modified -> {
                    modifiedAck.value |= modified;
                    return modified;
                })
                .map(modified -> persistentObject)
                .concatMapDelayError(new DeleteDataInVolumes(vertxContext, excludes))
                .map(modified -> {
                    modifiedAck.value |= modified;
                    return modified;
                })
                .map(modified -> persistentObject)
                .map(new PruneVersions(excludes))
                .map(modified -> {
                    modifiedAck.value |= modified;
                    return modified;
                })
                .singleOrDefault(modifiedAck.value)
                .map(modified -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end prune object=" + persistentObject.getId());
                    }
                    return modified;
                });
    }
}
