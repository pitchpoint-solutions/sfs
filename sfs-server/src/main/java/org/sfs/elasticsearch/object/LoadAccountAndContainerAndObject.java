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

import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.validate.ValidatePersistentAccountExists;
import org.sfs.validate.ValidatePersistentContainerExists;
import org.sfs.validate.ValidatePersistentObjectExists;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentObject;
import rx.Observable;
import rx.functions.Func1;

import static rx.Observable.just;

public class LoadAccountAndContainerAndObject implements Func1<ObjectPath, Observable<PersistentObject>> {

    private final VertxContext<Server> vertxContext;

    public LoadAccountAndContainerAndObject(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<PersistentObject> call(ObjectPath objectPath) {
        String accountId = objectPath.accountPath().get();
        String containerId = objectPath.containerPath().get();
        String objectId = objectPath.objectPath().get();
        return just(accountId)
                .flatMap(new LoadAccount(vertxContext))
                .map(new ValidatePersistentAccountExists())
                .flatMap(persistentAccount ->
                        just(containerId)
                                .flatMap(new LoadContainer(vertxContext, persistentAccount))
                                .map(new ValidatePersistentContainerExists())
                                .flatMap(persistentContainer ->
                                        just(objectId)
                                                .flatMap(new LoadObject(vertxContext, persistentContainer))
                                                .map(new ValidatePersistentObjectExists())));
    }
}
