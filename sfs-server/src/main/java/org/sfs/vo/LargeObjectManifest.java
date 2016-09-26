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

package org.sfs.vo;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.io.BaseEncoding.base16;
import static java.util.Collections.unmodifiableList;

public abstract class LargeObjectManifest<T extends LargeObjectManifest> {

    private List<Entry> entries = new ArrayList<>();

    public LargeObjectManifest() {
    }

    public List<Entry> getEntries() {
        return unmodifiableList(entries);
    }

    public T setEntries(Iterable<Entry> entries) {
        this.entries.clear();
        addAll(this.entries, entries);
        return (T) this;
    }

    public T merge(JsonArray document) {
        this.entries.clear();
        for (Object o : document) {
            JsonObject jsonObject = (JsonObject) o;
            Entry entry = new Entry().merge(jsonObject);
            entries.add(entry);
        }
        return (T) this;
    }

    public JsonArray toJsonObject() {
        JsonArray entryJsonArray = new JsonArray();
        for (Entry entry : entries) {
            entryJsonArray.add(entry.toJsonObject());
        }
        return entryJsonArray;
    }

    public static class Entry {
        private String path;
        private byte[] etag;
        private Long contentLength;

        public Entry() {
        }

        public Optional<String> getPath() {
            return fromNullable(path);
        }

        public Entry setPath(String path) {
            this.path = path;
            return this;
        }

        public Optional<Long> getContentLength() {
            return fromNullable(contentLength);
        }

        public Entry setContentLength(Long contentLength) {
            this.contentLength = contentLength;
            return this;
        }

        public Optional<byte[]> getEtag() {
            return fromNullable(etag);
        }

        public Entry setEtag(byte[] etag) {
            this.etag = etag;
            return this;
        }

        public Entry merge(JsonObject document) {
            setPath(document.getString("path"));
            setContentLength(document.getLong("size_bytes"));
            String etag = document.getString("etag");
            if (etag != null) {
                setEtag(base16().lowerCase().decode(etag));
            }
            return this;
        }

        public JsonObject toJsonObject() {
            JsonObject document = new JsonObject();
            document.put("path", path);
            if (etag != null) {
                document.put("etag", base16().lowerCase().encode(etag));
            } else {
                document.put("etag", (String) null);
            }
            document.put("size_bytes", contentLength);

            return document;
        }
    }
}
