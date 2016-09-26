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

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.all.blobreference.AcknowledgeBlobReference;
import org.sfs.rx.Defer;
import org.sfs.vo.TransientSegment;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static rx.Observable.from;
import static rx.Observable.just;

public class AcknowledgeSegment implements Func1<TransientSegment, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(AcknowledgeSegment.class);
    private final VertxContext<Server> vertxContext;

    public AcknowledgeSegment(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Boolean> call(final TransientSegment transientSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin acknowledge object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
        }
        if (transientSegment.isTinyData()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("end acknowledge object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
            }
            return just(true);
        } else {
            return from(transientSegment.verifiedUnAckdBlobs())
                    .flatMap(transientBlobReference ->
                            Defer.just(transientBlobReference)
                                    .flatMap(new AcknowledgeBlobReference(vertxContext))
                                    .map(ackd -> {
                                        transientBlobReference.setAcknowledged(ackd);
                                        if (ackd && LOGGER.isDebugEnabled()) {
                                            LOGGER.debug("marked acknowledge object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
                                        }
                                        return ackd;
                                    }))
                    .filter(ackd -> ackd)
                    .count()
                    .map(numberOfAcks -> numberOfAcks > 0)
                    .map(ackd -> {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("end acknowledge object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
                        }
                        return ackd;
                    });
        }
    }
}
