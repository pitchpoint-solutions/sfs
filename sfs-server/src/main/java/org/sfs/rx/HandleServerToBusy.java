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

package org.sfs.rx;

import com.google.common.base.Optional;
import org.sfs.filesystem.volume.VolumeToBusyExecutionException;
import org.sfs.util.HttpStatusCodeServerBusyException;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.util.ExceptionHelper.unwrapCause;
import static rx.Observable.error;

public class HandleServerToBusy<R> implements Func1<Throwable, Observable<R>> {

    public HandleServerToBusy() {
    }

    @Override
    public Observable<R> call(Throwable throwable) {
        Optional<VolumeToBusyExecutionException> oe = unwrapCause(VolumeToBusyExecutionException.class, throwable);
        if (oe.isPresent()) {
            return error(new HttpStatusCodeServerBusyException(oe.get()));
        } else {
            return error(throwable);
        }
    }
}
