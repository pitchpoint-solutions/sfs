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
import org.sfs.vo.TransientVersion;
import org.sfs.vo.XObject;
import org.sfs.vo.XVersion;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.rx.Defer.just;

public class PersistOrUpdateVersion implements Func1<XVersion, Observable<TransientVersion>> {

    private final VertxContext<Server> vertxContext;

    public PersistOrUpdateVersion(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<TransientVersion> call(XVersion xVersion) {
        final long versionId = xVersion.getId();
        XObject xObject = xVersion.getParent();
        return just(xObject)
                .flatMap(new PersistOrUpdateObject(vertxContext))
                .map(persistentObject -> persistentObject.getVersion(versionId).get());
    }
}
