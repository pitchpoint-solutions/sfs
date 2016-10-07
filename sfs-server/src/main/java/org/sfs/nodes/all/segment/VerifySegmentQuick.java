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

package org.sfs.nodes.all.segment;

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.Nodes;
import org.sfs.vo.TransientSegment;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.Iterables.size;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.just;

public class VerifySegmentQuick implements Func1<TransientSegment, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(VerifySegmentQuick.class);
    private final VertxContext<Server> vertxContext;
    private final Nodes nodes;

    public VerifySegmentQuick(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        this.nodes = vertxContext.verticle().nodes();
    }

    @Override
    public Observable<Boolean> call(TransientSegment transientSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin verifysegmentquick object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
        }
        if (transientSegment.isTinyData()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("end verifysegmentquick object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId() + ", verified=" + true);
            }
            return just(true);
        } else {
            int primaries = nodes.getNumberOfPrimaries();
            int replicas = nodes.getNumberOfReplicas();
            boolean verified =
                    size(transientSegment.verifiedAckdPrimaryBlobs()) >= primaries
                            && size(transientSegment.verifiedAckdReplicaBlobs()) >= replicas;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("end verifysegmentquick object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId() + ", verified=" + verified);
            }
            return just(verified);
        }
    }
}