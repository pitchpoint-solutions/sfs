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
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import rx.functions.Func1;

import java.util.Iterator;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class PruneDeletedBlobs implements Func1<TransientSegment, Boolean> {

    private static final Logger LOGGER = getLogger(PruneDeletedBlobs.class);

    @Override
    public Boolean call(TransientSegment transientSegment) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin prunedeletedblobs object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
        }
        Iterator<TransientBlobReference> blobIterator = transientSegment.getBlobs().iterator();
        boolean modified = false;
        while (blobIterator.hasNext()) {
            TransientBlobReference blob = blobIterator.next();
            if (blob.isDeleted()) {
                blobIterator.remove();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("removed prunedeletedblobs object=" + blob.getSegment().getParent().getParent().getId() + ", version=" + blob.getSegment().getParent().getId() + ", segment=" + blob.getSegment().getId() + ", volume=" + blob.getVolumeId() + ", position=" + blob.getPosition());
                }
                modified |= true;
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end prunedeletedblobs object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
        }
        return modified;
    }
}
