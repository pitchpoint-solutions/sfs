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
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.elasticsearch.object.PrefixSearch;
import org.sfs.validate.ValidatePersistentContainerExists;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.rx.Defer.just;
import static org.sfs.vo.ObjectPath.fromPaths;
import static rx.Observable.empty;

public class EmitDynamicLargeObjectParts implements Func1<TransientVersion, Observable<PersistentObject>> {

    private final VertxContext<Server> vertxContext;

    public EmitDynamicLargeObjectParts(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<PersistentObject> call(final TransientVersion transientVersion) {
        PersistentAccount persistentAccount = transientVersion.getParent().getParent().getParent();
        final ObjectPath objectPath = fromPaths(transientVersion.getParent().getId());

        Optional<String> objectManifest = transientVersion.getObjectManifest();

        if (objectManifest.isPresent()) {
            final ObjectPath dynamicLargeObjectPathPrefix = fromPaths(objectPath.accountName().get(), objectManifest.get());
            return just(dynamicLargeObjectPathPrefix.containerPath().get())
                    .flatMap(new LoadContainer(vertxContext, persistentAccount))
                    .map(new ValidatePersistentContainerExists())
                    .flatMap(persistentContainer ->
                            just(dynamicLargeObjectPathPrefix.objectPath().get())
                                    .flatMap(new PrefixSearch(vertxContext, persistentContainer)));
        }
        return empty();

    }
}
