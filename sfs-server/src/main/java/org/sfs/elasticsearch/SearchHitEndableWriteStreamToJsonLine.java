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

package org.sfs.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.EndableWriteStream;

import java.nio.charset.Charset;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class SearchHitEndableWriteStreamToJsonLine implements EndableWriteStream<SearchHit> {

    private static final Logger LOGGER = getLogger(SearchHitEndableWriteStreamToJsonLine.class);
    private final BufferEndableWriteStream bufferStreamConsumer;
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Charset charset = UTF_8;
    private final byte[] delimiter = "\n".getBytes(charset);
    private Handler<Throwable> exceptionHandler;

    static {
        mapper.configure(INDENT_OUTPUT, false);
    }

    public SearchHitEndableWriteStreamToJsonLine(BufferEndableWriteStream bufferStreamConsumer) {
        this.bufferStreamConsumer = bufferStreamConsumer;
    }

    @Override
    public boolean isEnded() {
        return bufferStreamConsumer != null && bufferStreamConsumer.isEnded();
    }

    @Override
    public SearchHitEndableWriteStreamToJsonLine drainHandler(Handler<Void> handler) {
        bufferStreamConsumer.drainHandler(handler);
        return this;
    }

    @Override
    public SearchHitEndableWriteStreamToJsonLine write(SearchHit data) {
        write0(data);
        return this;
    }

    @Override
    public SearchHitEndableWriteStreamToJsonLine exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        bufferStreamConsumer.exceptionHandler(handler);
        return this;
    }

    @Override
    public SearchHitEndableWriteStreamToJsonLine setWriteQueueMaxSize(int maxSize) {
        bufferStreamConsumer.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return bufferStreamConsumer.writeQueueFull();
    }

    @Override
    public SearchHitEndableWriteStreamToJsonLine endHandler(Handler<Void> endHandler) {
        bufferStreamConsumer.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(SearchHit data) {
        write0(data);
        bufferStreamConsumer.end();
    }

    @Override
    public void end() {
        bufferStreamConsumer.end();
    }

    protected void handleError(Throwable e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }

    }

    protected void write0(SearchHit data) {
        try {
            JsonObject jsonObject = new JsonObject()
                    .put("_index", data.getIndex())
                    .put("_type", data.getType())
                    .put("_id", data.getId())
                    .put("_source", new JsonObject(data.getSource()));
            String source = jsonObject.encode();
            checkState(!source.contains("\n"), "Record contains newline");
            byte[] bytes = source.getBytes(charset);
            bufferStreamConsumer.write(
                    buffer(bytes.length + delimiter.length)
                            .appendBytes(bytes)
                            .appendBytes(delimiter));
        } catch (Throwable e) {
            handleError(e);
        }
    }
}
