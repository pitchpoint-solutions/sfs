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

package org.sfs.elasticsearch;

import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToJson;
import static org.sfs.protobuf.XVolume.XIndexBlock;

public class Jsonify {

    public static String toString(XIndexBlock header) {
        return new JsonObject()
                .put("deleted", header.getDeleted())
                .put("acknowledged", header.getAcknowledged())
                .put("data_length", header.getDataLength())
                .put("data_position", header.getDataPosition())
                .put("update_ts", header.getUpdatedTs())
                .encodePrettily();
    }

    public static String toString(MasterNodeReadOperationRequestBuilder builder) {
        return builder.request().toString();
    }

    public static String toString(AcknowledgedRequestBuilder builder) {
        return builder.request().toString();
    }

    public static String toString(ActionRequestBuilder builder) {
        return builder.request().toString();
    }


    public static String toString(SearchRequestBuilder builder) {
        return toString(builder.request().source());
    }

    public static String toString(AcknowledgedResponse builder) {
        return new JsonObject().put("acknowledged", builder.isAcknowledged()).encodePrettily();
    }

    public static String toString(DeleteResponse deleteResponse) {
        return new JsonObject().put("_index", deleteResponse.getIndex())
                .put("_type", deleteResponse.getType())
                .put("_id", deleteResponse.getId())
                .put("_version", deleteResponse.getVersion())
                .put("found", deleteResponse.isFound())
                .encodePrettily();
    }

    public static String toString(IndexResponse indexResponse) {
        return new JsonObject().put("_index", indexResponse.getIndex())
                .put("_type", indexResponse.getType())
                .put("_id", indexResponse.getId())
                .put("_version", indexResponse.getVersion())
                .put("created", indexResponse.isCreated())
                .encodePrettily();

    }

    public static String toString(IndicesExistsResponse response) {
        return new JsonObject().put("exists", response.isExists())
                .encodePrettily();

    }

    public static String toString(ToXContent toXContent) {
        return XContentHelper.toString(toXContent, EMPTY_PARAMS);
    }


    public static String toString(BytesReference bytesReference) {
        try {
            return convertToJson(bytesReference, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
