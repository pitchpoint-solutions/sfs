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

package org.sfs.nodes.all.versions;

import io.vertx.core.logging.Logger;
import org.sfs.nodes.all.segment.PruneDeletedBlobs;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientVersion;
import rx.functions.Func1;

import java.util.Iterator;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class PruneDeletedSegments implements Func1<TransientVersion, Boolean> {

    private static final Logger LOGGER = getLogger(PruneDeletedSegments.class);

    @Override
    public Boolean call(TransientVersion transientVersion) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin prunedeletedsegments object=" + transientVersion.getParent().getId() + ", version=" + transientVersion.getId());
        }
        Iterator<TransientSegment> segmentIterator = transientVersion.getSegments().iterator();
        PruneDeletedBlobs pruneDeletedBlobs = new PruneDeletedBlobs();
        boolean modified = false;
        while (segmentIterator.hasNext()) {
            TransientSegment segment = segmentIterator.next();
            if (!segment.isTinyData()) {
                modified |= pruneDeletedBlobs.call(segment);
                if (segment.getBlobs().isEmpty()) {
                    segmentIterator.remove();
                    modified |= true;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("removed prunedeletedsegments object=" + segment.getParent().getParent().getId() + ", version=" + segment.getParent().getId() + ", segment=" + segment.getId());
                    }
                }
            } else {
                if (segment.isTinyDataDeleted()) {
                    segmentIterator.remove();
                    modified |= true;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("removed prunedeletedsegments object=" + segment.getParent().getParent().getId() + ", version=" + segment.getParent().getId() + ", segment=" + segment.getId());
                    }
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end prunedeletedsegments object=" + transientVersion.getParent().getId() + ", version=" + transientVersion.getId());
        }
        return modified;
    }
}
