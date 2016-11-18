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

package org.sfs.nodes.all.blobreference;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.XNode;
import org.sfs.rx.Defer;
import org.sfs.vo.TransientBlobReference;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.just;

public class DeleteBlobReference implements Func1<TransientBlobReference, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(DeleteBlobReference.class);
    private VertxContext<Server> vertxContext;

    public DeleteBlobReference(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Boolean> call(TransientBlobReference transientBlobReference) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin delete blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition());
        }
        ClusterInfo clusterInfo = vertxContext.verticle().getClusterInfo();
        return just(transientBlobReference)
                .filter(transientBlobReference1 -> transientBlobReference1.getVolumeId().isPresent() && transientBlobReference1.getPosition().isPresent())
                .flatMap(transientBlobReference1 -> {
                    String volumeId = transientBlobReference1.getVolumeId().get();
                    long position = transientBlobReference1.getPosition().get();
                    Optional<XNode> oXNode = clusterInfo.getNodeForVolume(vertxContext, volumeId);
                    if (!oXNode.isPresent()) {
                        LOGGER.warn("No nodes contain volume " + volumeId);
                        return Defer.just(false);
                    } else {
                        XNode xNode = oXNode.get();
                        return xNode.delete(volumeId, position)
                                .map(headerBlobOptional -> {
                                    if (headerBlobOptional.isPresent()) {
                                        return true;
                                    } else {
                                        return false;
                                    }
                                });
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("Delete fail blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition(), throwable);
                    return Defer.just(false);
                })
                .singleOrDefault(false)
                .map(modified -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end delete blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition() + ", deleted=" + modified);
                    }
                    return modified;
                });
    }
}