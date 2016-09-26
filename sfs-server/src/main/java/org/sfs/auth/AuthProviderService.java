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

import java.util.Iterator;
import java.util.ServiceLoader;

import static java.util.ServiceLoader.load;

public class AuthProviderService {

    private ServiceLoader<AuthProvider> loader;
    private AuthProvider selected;

    public AuthProviderService() {
        loader = load(AuthProvider.class);
    }

    protected AuthProvider selectWithHighestPriority() {
        if (selected == null) {
            Iterator<AuthProvider> iterator = loader.iterator();
            while (iterator.hasNext()) {
                AuthProvider next = iterator.next();
                if (selected == null) {
                    selected = next;
                } else if (next.priority() > selected.priority()) {
                    selected = next;
                }
            }
        }
        return selected;
    }

    public int priority() {
        return selectWithHighestPriority().priority();
    }

    public Observable<Void> authenticate(SfsRequest sfsRequest) {
        return selectWithHighestPriority().authenticate(sfsRequest);
    }

    public Observable<Boolean> isAuthenticated(SfsRequest sfsRequest) {
        return selectWithHighestPriority().isAuthenticated(sfsRequest);
    }

    public void handleOpenstackKeystoneAuth(SfsRequest sfsRequest) {
        selectWithHighestPriority().handleOpenstackKeystoneAuth(sfsRequest);
    }

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        return selectWithHighestPriority().open(vertxContext);
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return selectWithHighestPriority().close(vertxContext);
    }

    public Observable<Boolean> canObjectUpdate(SfsRequest sfsRequest, TransientVersion version) {
        return selectWithHighestPriority().canObjectUpdate(sfsRequest, version);
    }

    public Observable<Boolean> canObjectDelete(SfsRequest sfsRequest, TransientVersion version) {
        return selectWithHighestPriority().canObjectDelete(sfsRequest, version);
    }

    public Observable<Boolean> canObjectCreate(SfsRequest sfsRequest, TransientVersion version) {
        return selectWithHighestPriority().canObjectCreate(sfsRequest, version);
    }

    public Observable<Boolean> canObjectRead(SfsRequest sfsRequest, TransientVersion version) {
        return selectWithHighestPriority().canObjectRead(sfsRequest, version);
    }

    public Observable<Boolean> canContainerUpdate(SfsRequest sfsRequest, PersistentContainer container) {
        return selectWithHighestPriority().canContainerUpdate(sfsRequest, container);
    }

    public Observable<Boolean> canContainerDelete(SfsRequest sfsRequest, PersistentContainer version) {
        return selectWithHighestPriority().canContainerDelete(sfsRequest, version);
    }

    public Observable<Boolean> canContainerCreate(SfsRequest sfsRequest, TransientContainer version) {
        return selectWithHighestPriority().canContainerCreate(sfsRequest, version);
    }

    public Observable<Boolean> canContainerListObjects(SfsRequest sfsRequest, PersistentContainer version) {
        return selectWithHighestPriority().canContainerListObjects(sfsRequest, version);
    }

    public Observable<Boolean> canContainerRead(SfsRequest sfsRequest, PersistentContainer version) {
        return selectWithHighestPriority().canContainerRead(sfsRequest, version);
    }

    public Observable<Boolean> canAccountUpdate(SfsRequest sfsRequest, PersistentAccount version) {
        return selectWithHighestPriority().canAccountUpdate(sfsRequest, version);
    }

    public Observable<Boolean> canAccountDelete(SfsRequest sfsRequest, PersistentAccount version) {
        return selectWithHighestPriority().canAccountDelete(sfsRequest, version);
    }

    public Observable<Boolean> canAccountCreate(SfsRequest sfsRequest, TransientAccount version) {
        return selectWithHighestPriority().canAccountCreate(sfsRequest, version);
    }

    public Observable<Boolean> canAccountRead(SfsRequest sfsRequest, PersistentAccount version) {
        return selectWithHighestPriority().canAccountRead(sfsRequest, version);
    }

    public Observable<Boolean> canAdmin(SfsRequest sfsRequest) {
        return selectWithHighestPriority().canAdmin(sfsRequest);
    }
}
