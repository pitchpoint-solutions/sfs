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

package org.sfs.integration.java.func;

import org.sfs.Server;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

public class RunJobs implements Func1<Void, Observable<Void>> {

    private final VertxContext<Server> vertxContext;

    public RunJobs(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        return vertxContext.verticle().jobs().run(vertxContext);
    }
}

