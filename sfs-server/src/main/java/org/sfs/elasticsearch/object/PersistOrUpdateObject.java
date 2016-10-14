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

package org.sfs.elasticsearch.object;

import com.google.common.base.Optional;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientObject;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.XObject;
import rx.Observable;
import rx.functions.Func1;

import static java.util.Calendar.getInstance;
import static org.sfs.rx.Defer.just;

public class PersistOrUpdateObject implements Func1<XObject, Observable<PersistentObject>> {

    private final VertxContext<Server> vertxContext;

    public PersistOrUpdateObject(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<PersistentObject> call(XObject xObject) {
        if (xObject instanceof PersistentObject) {
            return just((PersistentObject) xObject)
                    .map(persistentObject -> persistentObject.setUpdateTs(getInstance()))
                    .flatMap(new UpdateObject(vertxContext))
                    .map(new ValidateOptimisticObjectLock());
        } else {
            return just((TransientObject) xObject)
                    .doOnNext(transientObject -> {
                        Optional<TransientServiceDef> currentMaintainerNode = vertxContext.verticle().getClusterInfo().getCurrentMaintainerNode();
                        if (currentMaintainerNode.isPresent()) {
                            transientObject.setNodeId(currentMaintainerNode.get().getId());
                        }
                    })
                    .flatMap(new PersistObject(vertxContext))
                    .map(new ValidateOptimisticObjectLock());
        }
    }
}
