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

package org.sfs.metadata;

import com.google.common.collect.SortedSetMultimap;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Splitter.on;
import static com.google.common.collect.TreeMultimap.create;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Collections.emptyList;
import static org.sfs.protobuf.XVolume.XDumpFile;
import static org.sfs.protobuf.XVolume.XDumpFile.Metadata.Builder;
import static org.sfs.protobuf.XVolume.XDumpFile.Metadata.Entry;
import static org.sfs.protobuf.XVolume.XDumpFile.Metadata.newBuilder;
import static org.sfs.util.SfsHttpHeaders.X_ADD_ACCOUNT_META;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META;
import static org.sfs.util.SfsHttpHeaders.X_ADD_OBJECT_META;
import static org.sfs.util.SfsHttpHeaders.X_REMOVE_ACCOUNT_META;
import static org.sfs.util.SfsHttpHeaders.X_REMOVE_CONTAINER_META;
import static org.sfs.util.SfsHttpHeaders.X_REMOVE_OBJECT_META;

public class Metadata {

    private static final SortedSet<String> EMPTY_SET = new TreeSet<>();
    private SortedSetMultimap<String, String> meta;
    private final Pattern addParser;
    private final Pattern removeParser;

    private Metadata(Pattern addParser, Pattern removeParser) {
        this.addParser = addParser;
        this.removeParser = removeParser;
        this.meta = create(CASE_INSENSITIVE_ORDER, CASE_INSENSITIVE_ORDER);
    }

    public static Metadata account() {
        return new Metadata(X_ADD_ACCOUNT_META, X_REMOVE_ACCOUNT_META);
    }

    public static Metadata container() {
        return new Metadata(X_ADD_CONTAINER_META, X_REMOVE_CONTAINER_META);
    }

    public static Metadata object() {
        return new Metadata(X_ADD_OBJECT_META, X_REMOVE_OBJECT_META);
    }

    public SortedSet<String> get(String key) {
        SortedSet<String> list = meta.get(key);
        if (list == null) list = EMPTY_SET;
        return list;
    }

    public boolean put(String key, String value) {
        return meta.put(key, value);
    }

    public boolean remove(String key, String value) {
        return meta.remove(key, value);
    }

    public void removeAll(String key) {
        meta.removeAll(key);
    }

    public Metadata clear() {
        meta.clear();
        return this;
    }

    public Set<String> keySet() {
        return meta.keySet();
    }

    public XDumpFile.Metadata toExportObject() {
        Builder builder = newBuilder();
        if (meta != null) {
            for (Map.Entry<String, Collection<String>> entry : meta.asMap().entrySet()) {
                String name = entry.getKey();
                Collection<String> values = entry.getValue();
                Entry metadataEntry =
                        Entry.newBuilder()
                                .setName(name)
                                .addAllValues(values != null ? values : emptyList())
                                .build();
                builder = builder.addEntries(metadataEntry);
            }
        }
        return builder.build();
    }

    public JsonArray toJsonObject() {
        JsonArray metadata = new JsonArray();
        if (!meta.isEmpty()) {
            for (String key : keySet()) {
                SortedSet<String> values = get(key);
                if (!values.isEmpty()) {
                    JsonArray valueJsonArray = new JsonArray();
                    for (String value : values) {
                        valueJsonArray.add(value);
                    }
                    JsonObject metaObject = new JsonObject();
                    metaObject.put("name", key);
                    metaObject.put("values", valueJsonArray);

                    metadata.add(metaObject);
                }
            }
        }
        return metadata;
    }

    public Metadata withExportObject(XDumpFile.Metadata metadataArray) {
        for (Entry entry : metadataArray.getEntriesList()) {
            String name = entry.getName();
            List<String> metaValues = entry.getValuesList();
            for (String metaValue : metaValues) {
                put(name, metaValue);
            }
        }
        return this;
    }

    public Metadata withJsonObject(JsonArray metadataArray) {
        for (Object o : metadataArray) {
            JsonObject jsonObject = (JsonObject) o;
            String name = jsonObject.getString("name");
            JsonArray valueArray = jsonObject.getJsonArray("values", new JsonArray());
            for (Object p : valueArray) {
                String metaValue = (String) p;
                put(name, metaValue);
            }

        }
        return this;
    }

    public Metadata withHttpHeaders(MultiMap headers) {
        Set<String> processed = new HashSet<>();
        // adds first
        for (String headerName : headers.names()) {
            Matcher matcher = addParser.matcher(headerName);
            if (matcher.find()) {
                List<String> values = headers.getAll(headerName);
                if (values != null && !values.isEmpty()) {
                    String metaName = matcher.group(1);
                    removeAll(metaName);
                    for (String value : values) {
                        for (String split :
                                on(',')
                                        .omitEmptyStrings()
                                        .trimResults()
                                        .split(value)) {
                            processed.add(metaName);
                            put(metaName, split);
                        }
                    }
                }
            }
        }
        // then deletes
        for (String headerName : headers.names()) {
            Matcher matcher0 = addParser.matcher(headerName);
            if (matcher0.find()) {
                String metaName = matcher0.group(1);
                if (processed.add(metaName)) {
                    List<String> values = headers.getAll(headerName);
                    boolean hasData = false;
                    if (values != null) {
                        for (String value : values) {
                            if (value != null) {
                                value = value.trim();
                                if (!value.isEmpty()) {
                                    hasData = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!hasData) {
                        removeAll(metaName);
                    }
                }
            }
            Matcher matcher1 = removeParser.matcher(headerName);
            if (matcher1.find()) {
                String metaName = matcher1.group(1);
                if (processed.add(metaName)) {
                    removeAll(metaName);
                }
            }
        }
        return this;
    }
}
