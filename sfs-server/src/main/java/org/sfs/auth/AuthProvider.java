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

package org.sfs.auth;

import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.TransientAccount;
import org.sfs.vo.TransientContainer;
import org.sfs.vo.TransientVersion;
import rx.Observable;

public interface AuthProvider {

    int priority();

    Observable<Void> authenticate(SfsRequest sfsRequest);

    Observable<Boolean> isAuthenticated(SfsRequest sfsRequest);

    void handleOpenstackKeystoneAuth(SfsRequest sfsRequest);

    Observable<Void> open(VertxContext<Server> vertxContext);

    Observable<Void> close(VertxContext<Server> vertxContext);

    Observable<Boolean> canObjectUpdate(SfsRequest sfsRequest, TransientVersion version);

    Observable<Boolean> canObjectDelete(SfsRequest sfsRequest, TransientVersion version);

    Observable<Boolean> canObjectCreate(SfsRequest sfsRequest, TransientVersion version);

    Observable<Boolean> canObjectRead(SfsRequest sfsRequest, TransientVersion version);

    Observable<Boolean> canContainerUpdate(SfsRequest sfsRequest, PersistentContainer container);

    Observable<Boolean> canContainerDelete(SfsRequest sfsRequest, PersistentContainer container);

    Observable<Boolean> canContainerListObjects(SfsRequest sfsRequest, PersistentContainer container);

    Observable<Boolean> canContainerCreate(SfsRequest sfsRequest, TransientContainer container);

    Observable<Boolean> canContainerRead(SfsRequest sfsRequest, PersistentContainer container);

    Observable<Boolean> canAccountUpdate(SfsRequest sfsRequest, PersistentAccount account);

    Observable<Boolean> canAccountDelete(SfsRequest sfsRequest, PersistentAccount account);

    Observable<Boolean> canAccountCreate(SfsRequest sfsRequest, TransientAccount account);

    Observable<Boolean> canAccountRead(SfsRequest sfsRequest, PersistentAccount account);

    Observable<Boolean> canAdmin(SfsRequest sfsRequest);
}
