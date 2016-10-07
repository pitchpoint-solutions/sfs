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

package org.sfs.nodes.compute.account;

import com.google.common.net.MediaType;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.SfsRequest;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.account.ListContainers;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.io.BufferOutputStream;
import org.sfs.metadata.Metadata;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateAccountPath;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidatePersistentAccountExists;
import org.sfs.vo.Account;
import org.sfs.vo.ContainerList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.SortedSet;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Ordering.from;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.APPLICATION_XML_UTF_8;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static com.google.common.net.MediaType.parse;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.net.HttpURLConnection.HTTP_OK;
import static javax.xml.stream.XMLOutputFactory.newFactory;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_BYTES_USED;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_CONTAINER_COUNT;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_OBJECT_COUNT;
import static org.sfs.util.SfsHttpHeaders.X_ADD_ACCOUNT_META_PREFIX;
import static org.sfs.util.SfsHttpQueryParams.FORMAT;
import static org.sfs.vo.ContainerList.SparseContainer;
import static org.sfs.vo.ObjectPath.fromPaths;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class GetAccount implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(GetAccount.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateAccountPath())
                .map(objectPath -> objectPath.accountPath().get())
                .flatMap(new LoadAccount(httpServerRequest.vertxContext()))
                .map(new ValidatePersistentAccountExists())
                .flatMap(new ListContainers(httpServerRequest))
                .single()
                .subscribe(new ConnectionCloseTerminus<ContainerList>(httpServerRequest) {

                    @Override
                    public void onNext(ContainerList containerList) {

                        HttpServerResponse httpServerResponse = httpServerRequest.response();
                        MultiMap headerParams = httpServerRequest.headers();
                        MultiMap queryParams = httpServerRequest.params();
                        String format = queryParams.get(FORMAT);

                        String accept = headerParams.get(ACCEPT);

                        Account account = containerList.getAccount();

                        Metadata metadata = account.getMetadata();

                        for (String key : metadata.keySet()) {
                            SortedSet<String> values = metadata.get(key);
                            if (values != null && !values.isEmpty()) {
                                httpServerResponse.putHeader(format("%s%s", X_ADD_ACCOUNT_META_PREFIX, key), values);
                            }
                        }

                        httpServerResponse.putHeader(X_ACCOUNT_OBJECT_COUNT, valueOf(containerList.getObjectCount()));
                        httpServerResponse.putHeader(X_ACCOUNT_CONTAINER_COUNT, valueOf(containerList.getContainerCount()));
                        httpServerResponse.putHeader(
                                X_ACCOUNT_BYTES_USED,
                                BigDecimal.valueOf(containerList.getBytesUsed())
                                        .setScale(0, ROUND_HALF_UP)
                                        .toString()
                        );

                        MediaType parsedAccept = null;
                        if (!isNullOrEmpty(accept)) {
                            parsedAccept = parse(accept);
                        }

                        if (equalsIgnoreCase("xml", format)) {
                            parsedAccept = APPLICATION_XML_UTF_8;
                        } else if (equalsIgnoreCase("json", format)) {
                            parsedAccept = JSON_UTF_8;
                        }

                        httpServerResponse.setStatusCode(HTTP_OK);

                        if (parsedAccept != null && JSON_UTF_8.is(parsedAccept)) {
                            String charset = UTF_8.toString();

                            JsonArray array = new JsonArray();

                            for (SparseContainer container : ordered(containerList.getContainers())) {

                                array.add(
                                        new JsonObject()
                                                .put("name", container.getContainerName())
                                                .put("count", container.getObjectCount())
                                                .put("bytes",
                                                        BigDecimal.valueOf(container.getByteCount())
                                                                .setScale(0, ROUND_HALF_UP)
                                                                .longValue()));

                            }

                            Buffer buffer = buffer(array.encode(), charset);
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, JSON_UTF_8.toString());
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_LENGTH, valueOf(buffer.length()));
                            httpServerResponse.write(buffer);

                        } else if (parsedAccept != null && APPLICATION_XML_UTF_8.is(parsedAccept)) {

                            BufferOutputStream bufferOutputStream = new BufferOutputStream();
                            String charset = UTF_8.toString();
                            XMLStreamWriter writer = null;
                            try {
                                writer = newFactory()
                                        .createXMLStreamWriter(bufferOutputStream, charset);

                                writer.writeStartDocument(charset, "1.0");

                                writer.writeStartElement("account");

                                writer.writeAttribute("name", fromPaths(account.getId()).accountName().get());

                                for (SparseContainer container : ordered(containerList.getContainers())) {

                                    writer.writeStartElement("container");

                                    writer.writeStartElement("name");
                                    writer.writeCharacters(container.getContainerName());
                                    writer.writeEndElement();

                                    writer.writeStartElement("count");
                                    writer.writeCharacters(valueOf(container.getObjectCount()));
                                    writer.writeEndElement();

                                    writer.writeStartElement("bytes");
                                    writer.writeCharacters(
                                            BigDecimal.valueOf(container.getByteCount())
                                                    .setScale(0, ROUND_HALF_UP)
                                                    .toString());
                                    writer.writeEndElement();

                                    writer.writeEndElement();
                                }

                                writer.writeEndElement();

                                writer.writeEndDocument();

                            } catch (XMLStreamException e) {
                                throw new RuntimeException(e);
                            } finally {
                                try {
                                    if (writer != null) {
                                        writer.close();
                                    }
                                } catch (XMLStreamException e) {
                                    LOGGER.warn(e.getLocalizedMessage(), e);
                                }
                            }

                            Buffer buffer = bufferOutputStream.toBuffer();
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, APPLICATION_XML_UTF_8.toString());
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_LENGTH, valueOf(buffer.length()));
                            httpServerResponse.write(buffer);

                        } else {
                            String charset = UTF_8.toString();
                            Buffer buffer = buffer();
                            for (SparseContainer container : ordered(containerList.getContainers())) {
                                buffer.appendString(container.getContainerName(), charset);
                                buffer.appendString("\n", charset);
                            }
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString());
                            httpServerResponse = httpServerResponse.putHeader(CONTENT_LENGTH, valueOf(buffer.length()));
                            httpServerResponse.write(buffer);

                        }
                    }

                });

    }

    protected Iterable<SparseContainer> ordered(Iterable<SparseContainer> iterable) {
        return from(new Comparator<SparseContainer>() {
            @Override
            public int compare(SparseContainer o1, SparseContainer o2) {
                return o1.getContainerName().compareTo(o2.getContainerName());
            }
        }).sortedCopy(iterable);
    }
}