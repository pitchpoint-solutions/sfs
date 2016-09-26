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

package org.sfs.nodes.all.segment;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.nodes.all.blobreference.GetBlobReferenceReadStream;
import org.sfs.rx.Holder2;
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Optional.fromNullable;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.sfs.rx.RxHelper.iterate;
import static rx.Observable.just;

public class GetSegmentReadStream implements Func1<TransientSegment, Observable<Optional<Holder2<TransientBlobReference, ReadStreamBlob>>>> {

    private static final Logger LOGGER = getLogger(GetSegmentReadStream.class);
    private VertxContext<Server> vertxContext;
    private boolean verified;

    public GetSegmentReadStream(VertxContext<Server> vertxContext) {
        this(vertxContext, false);
    }

    public GetSegmentReadStream(VertxContext<Server> vertxContext, boolean verified) {
        this.vertxContext = vertxContext;
        this.verified = verified;
    }

    @Override
    public Observable<Optional<Holder2<TransientBlobReference, ReadStreamBlob>>> call(TransientSegment transientSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin getsegmentreadstream object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
        }
        AtomicReference<Holder2<TransientBlobReference, ReadStreamBlob>> match = new AtomicReference<>();
        return iterate(
                // if we're going to be verifying ourselves don't bother looking in the metadata to see if the blob is verified
                verified ? transientSegment.getBlobs() : transientSegment.verifiedAckdBlobs(),
                transientBlobReference ->
                        just(transientBlobReference)
                                .flatMap(new GetBlobReferenceReadStream(vertxContext, verified))
                                .map(oReadStreamBlob -> {
                                    if (oReadStreamBlob.isPresent()) {
                                        match.set(new Holder2<>(transientBlobReference, oReadStreamBlob.get()));
                                        return FALSE;
                                    } else {
                                        return TRUE;
                                    }
                                }))
                .map(aborted -> fromNullable(match.get()))
                .doOnNext(holder2Optional -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end getsegmentreadstream object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
                    }
                });
    }
}
