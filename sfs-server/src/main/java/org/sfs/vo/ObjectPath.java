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

package org.sfs.vo;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import org.sfs.SfsRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.skip;
import static com.google.common.collect.Lists.newArrayList;
import static org.sfs.util.UrlScaper.unescape;

public class ObjectPath {

    private boolean hasTrailingDelimiter;
    public static final char DELIMITER = '/';
    public static final int DELIMITER_LENGTH = 1;
    private List<String> segments;

    public static ObjectPath fromSfsRequest(SfsRequest httpServerRequest) {
        return new ObjectPath(true, unescape(httpServerRequest.path()));
    }

    public static ObjectPath fromPaths(String path, String... paths) {
        return new ObjectPath(false, path, paths);
    }


    private ObjectPath(boolean stripContextRoot, String path, String... paths) {
        String lastPath = paths.length >= 1 ? paths[paths.length - 1] : path;
        this.hasTrailingDelimiter = !lastPath.isEmpty() && DELIMITER == lastPath.charAt(lastPath.length() - 1);
        this.segments = stripContextRoot ? newArrayList(skip(xform(path, paths), 1)) : newArrayList(xform(path, paths));
    }

    public Optional<String> accountName() {
        int size = segments.size();
        if (size >= 1) {
            return of(segments.get(0));
        } else {
            return absent();
        }
    }

    public Optional<String> accountPath() {
        if (segments.size() >= 1) {
            return of(DELIMITER + accountName().get());
        } else {
            return absent();
        }
    }

    public Optional<String> containerName() {
        int size = segments.size();
        if (size >= 2) {
            return of(segments.get(1));
        } else {
            return absent();
        }
    }

    public Optional<String> objectIndexNameV0() {
        int size = segments.size();
        if (size >= 2) {
            return of("sfs_v0_" + segments.get(1));
        } else {
            return absent();
        }
    }

    public Optional<String> containerPath() {
        if (segments.size() >= 2) {
            return of(DELIMITER + accountName().get() + DELIMITER + containerName().get());
        } else {
            return absent();
        }
    }

    public Optional<String> objectName() {
        int size = segments.size();
        if (size >= 3) {
            String p = on(DELIMITER).join(segments.subList(2, size));
            return of(p);
        } else {
            return absent();
        }
    }

    public Optional<String> objectPath() {
        if (segments.size() >= 3) {
            String p = DELIMITER + accountName().get() + DELIMITER + containerName().get() + DELIMITER + objectName().get();
            return of(p);
        } else {
            return absent();
        }
    }

    public ObjectPath append(String path, String... paths) {
        String lastPath = paths.length >= 1 ? paths[paths.length - 1] : path;
        hasTrailingDelimiter = !lastPath.isEmpty() && DELIMITER == lastPath.charAt(lastPath.length() - 1);
        addAll(this.segments, xform(path, paths));
        return this;
    }

    protected Iterable<String> xform(String path, String... paths) {

        List<String> p = new ArrayList<>();
        p.add(path);
        Collections.addAll(p, paths);

        FluentIterable<String> iteratable = from(p)
                .transformAndConcat(input -> Splitter.on(DELIMITER).split(input))
                .transform(input -> input != null ? input : null)
                .filter(notNull());

        if (path.charAt(0) == DELIMITER) {
            return iteratable.skip(1);
        } else {
            return iteratable;
        }
    }
}
