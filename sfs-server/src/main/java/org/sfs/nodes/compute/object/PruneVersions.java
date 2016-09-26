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

package org.sfs.nodes.compute.object;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.nodes.all.versions.PruneDeletedSegments;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import org.sfs.vo.XVersion;
import rx.functions.Func1;

import java.util.Iterator;
import java.util.Set;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.util.Collections.emptySet;

public class PruneVersions implements Func1<PersistentObject, Boolean> {

    private static final Logger LOGGER = getLogger(PruneVersions.class);

    private final Set<XVersion> excludes;

    public PruneVersions(Set<XVersion> excludes) {
        this.excludes = excludes;
    }

    public PruneVersions() {
        this(emptySet());
    }

    @Override
    public Boolean call(PersistentObject persistentObject) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin prunversions object=" + persistentObject.getId());
        }
        Iterator<TransientVersion> versionsIterator = persistentObject.getVersions().iterator();
        PruneDeletedSegments pruneDeletedSegments = new PruneDeletedSegments();
        boolean modified = false;
        while (versionsIterator.hasNext()) {
            TransientVersion version = versionsIterator.next();
            if (!excludes.contains(version)) {
                modified |= pruneDeletedSegments.call(version);
                Optional<Long> oContentLength = version.getContentLength();
                boolean isSafeToRemoveFromIndex = version.isSafeToRemoveFromIndex();
                boolean hasNonZeroContentLength = oContentLength.isPresent() && oContentLength.get() > 0;
                if ((isSafeToRemoveFromIndex && hasNonZeroContentLength) || (isSafeToRemoveFromIndex && version.isDeleted())) {
                    versionsIterator.remove();
                    modified |= true;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("removed pruneversions object=" + version.getParent().getId() + ", version=" + version.getId());
                    }
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end pruneversions object=" + persistentObject.getId());
        }
        return modified;
    }
}
