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

package org.sfs.elasticsearch.container;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.validate.ValidatePersistentAccountExists;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static rx.Observable.just;

public class LoadAccountAndOptionalContainer implements Func1<ObjectPath, Observable<Optional<PersistentContainer>>> {

    private static final Logger LOGGER = getLogger(LoadAccountAndOptionalContainer.class);
    private final VertxContext<Server> vertxContext;

    public LoadAccountAndOptionalContainer(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentContainer>> call(ObjectPath objectPath) {
        String accountId = objectPath.accountPath().get();
        String containerId = objectPath.containerPath().get();
        return just(accountId)
                .flatMap(new LoadAccount(vertxContext))
                .map(new ValidatePersistentAccountExists())
                .flatMap(persistentAccount ->
                        just(containerId)
                                .flatMap(new LoadContainer(vertxContext, persistentAccount)));
    }
}