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

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import org.sfs.vo.XVersion;
import rx.functions.Func1;

import java.util.NavigableSet;
import java.util.Set;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptySet;

public class ExpireVersions implements Func1<PersistentObject, Boolean> {

    private static final Logger LOGGER = getLogger(ExpireVersions.class);

    private final Set<XVersion> excludes;

    public ExpireVersions(Set<XVersion> excludes) {
        this.excludes = excludes;
    }

    public ExpireVersions() {
        this(emptySet());
    }

    @Override
    public Boolean call(PersistentObject persistentObject) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin expire object=" + persistentObject.getId());
        }
        boolean isDebugEnabled = LOGGER.isDebugEnabled();
        NavigableSet<TransientVersion> versions = persistentObject.getVersions();
        int revisionCount = versions.size();
        final long now = currentTimeMillis();
        PersistentContainer persistentContainer = persistentObject.getParent();
        final int maxObjectRevisions = persistentContainer.getMaxObjectRevisions();
        boolean modified = false;
        for (TransientVersion transientVersion : versions) {
            // delete revisions until util the revision count == max revisions
            if (!excludes.contains(transientVersion)) {
                if (maxObjectRevisions > 0 && revisionCount > maxObjectRevisions) {
                    transientVersion.setDeleted(TRUE);
                    if (isDebugEnabled) {
                        LOGGER.debug("Revision counter marked " + persistentObject.getId() + " version " + transientVersion.getId() + " as deleted");
                    }
                    modified |= true;
                } else {
                    // if this version is expired then this version must be deleted
                    Optional<Long> oDeleteAt = transientVersion.getDeleteAt();
                    if (oDeleteAt.isPresent()) {
                        long deleteAt = oDeleteAt.get();
                        if (now >= deleteAt) {
                            transientVersion.setDeleted(TRUE);
                            if (isDebugEnabled) {
                                LOGGER.debug("DeleteAt is " + deleteAt + ", now is " + now + ", Marked " + persistentObject.getId() + " version " + transientVersion.getId() + " as deleted");
                            }
                            modified |= true;
                        }
                    }
                }
            }
            revisionCount--;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("end expire object=" + persistentObject.getId());
        }
        return modified;
    }
}
