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

package org.sfs.util;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;

public class SfsHttpHeaders {

    public static final String X_SFS_REMOTE_NODE_TOKEN = "x-sfs-remote-node-token";
    public static final String X_AUTH_TOKEN = "X-Auth-Token";
    public static final String X_ADD_CONTAINER_META_PREFIX = "X-Container-Meta-";
    public static final String X_REMOVE_CONTAINER_META_PREFIX = "X-Remove-Container-Meta-";
    public static final String X_ADD_ACCOUNT_META_PREFIX = "X-Account-Meta-";
    public static final String X_REMOVE_ACCOUNT_META_PREFIX = "X-Remove-Account-Meta-";
    public static final String X_ADD_OBJECT_META_PREFIX = "X-Object-Meta-";
    public static final Pattern X_ADD_CONTAINER_META = compile("^X-Container-Meta-(.+)$", CASE_INSENSITIVE);
    public static final Pattern X_ADD_ACCOUNT_META = compile("^X-Account-Meta-(.+)$", CASE_INSENSITIVE);
    public static final Pattern X_ADD_OBJECT_META = compile("^X-Object-Meta-(.+)$", CASE_INSENSITIVE);
    public static final Pattern X_REMOVE_CONTAINER_META = compile("^X-Remove-Container-Meta-(.+)$", CASE_INSENSITIVE);
    public static final Pattern X_REMOVE_ACCOUNT_META = compile("^X-Remove-Account-Meta-(.+)$", CASE_INSENSITIVE);
    public static final Pattern X_REMOVE_OBJECT_META = compile("^X-Remove-Object-Meta-(.+)$", CASE_INSENSITIVE);
    public static final String X_DELETE_AT = "X-Delete-At";
    public static final String X_DELETE_AFTER = "X-Delete-After";
    public static final String X_SERVER_SIDE_ENCRYPTION = "X-Server-Side-Encryption";
    public static final String X_CONTENT_SHA512 = "X-Content-SHA512";
    public static final String X_CONTENT_COMPUTED_DIGEST_PREFIX = "X-Computed-Digest-";
    public static final String X_CONTENT_POSITION = "X-Content-Position";
    public static final String X_CONTENT_VOLUME = "X-Content-Volume";
    public static final String X_CONTENT_VOLUME_PRIMARY = "X-Content-Volume-Primary";
    public static final String X_CONTENT_VOLUME_REPLICA = "X-Content-Volume-Replica";
    public static final String X_CONTENT_OFFSET = "X-Content-Offset";
    public static final String X_CONTENT_LENGTH = "X-Content-Length";
    public static final String X_CONTENT_VERSION = "X-Content-Version";
    public static final String X_OBJECT_MANIFEST = "X-Object-Manifest";
    public static final String X_STATIC_LARGE_OBJECT = "X-Static-Large-Object";
    public static final String X_CONTAINER_OBJECT_COUNT = "X-Container-Object-Count";
    public static final String X_CONTAINER_BYTES_USED = "X-Container-Bytes-Used";
    public static final String X_ACCOUNT_OBJECT_COUNT = "X-Account-Object-Count";
    public static final String X_ACCOUNT_CONTAINER_COUNT = "X-Account-Container-Count";
    public static final String X_ACCOUNT_BYTES_USED = "X-Account-Bytes-Used";
    public static final String X_COPY_FROM = "X-Copy-From";
    public static final String X_CONTEXT_ROOT = "X-Context-Root";

    // general sfs params
    public static final String X_SFS_COPY_DEST_DIRECTORY = "x-sfs-copy-dest-directory";
    public static final String X_SFS_DEST_DIRECTORY = "x-sfs-dest-directory";
    public static final String X_SFS_SRC_DIRECTORY = "x-sfs-src-directory";
    public static final String X_SFS_IMPORT_SKIP_POSITIONS = "x-sfs-import-skip-positions";
    public static final String X_SFS_KEEP_ALIVE_TIMEOUT = "x-sfs-keep-alive-timeout";
    public static final String X_SFS_SECRET = "x-sfs-secret";
    public static final String X_SFS_COMPRESS = "x-sfs-compress";
    public static final String X_SFS_OBJECT_INDEX_SHARDS = "x-sfs-object-index-shards";
    public static final String X_SFS_OBJECT_INDEX_REPLICAS = "x-sfs-object-index-replicas";
    public static final String X_SFS_OBJECT_REPLICAS = "x-sfs-object-replicas";

}
