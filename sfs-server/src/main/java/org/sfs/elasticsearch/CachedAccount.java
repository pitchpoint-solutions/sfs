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

package org.sfs.elasticsearch;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.rx.Defer;
import org.sfs.vo.PersistentAccount;
import rx.Observable;

public class CachedAccount {

    private final VertxContext<Server> vertxContext;

    private final Cache<String, PersistentAccount> cache =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .build();

    public CachedAccount(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    public Observable<PersistentAccount> get(String accountId) {
        return Observable.defer(() -> {
            final PersistentAccount persistentAccount = cache.getIfPresent(accountId);
            if (persistentAccount == null) {
                return Defer.just(accountId)
                        .flatMap(new LoadAccount(vertxContext))
                        .map(oPersistentAccount -> {
                            if (oPersistentAccount.isPresent()) {
                                cache.put(accountId, oPersistentAccount.get());

                            }
                            return oPersistentAccount.get();
                        });
            } else {
                return Defer.just(persistentAccount);
            }
        });
    }
}
