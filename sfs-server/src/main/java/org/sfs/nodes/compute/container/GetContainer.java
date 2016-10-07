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

package org.sfs.nodes.compute.container;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.net.MediaType;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.container.ListObjects;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.elasticsearch.container.LoadContainerStats;
import org.sfs.io.BufferEndableWriteStreamOutputStream;
import org.sfs.io.HttpServerResponseEndableWriteStream;
import org.sfs.io.NoCloseOutputStream;
import org.sfs.metadata.Metadata;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionContainerListObjects;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.vo.ContainerStats;
import org.sfs.vo.ObjectList;
import org.sfs.vo.PersistentContainer;
import rx.Observable;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.SortedSet;

import static com.fasterxml.jackson.core.JsonEncoding.UTF8;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Ordering.from;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.APPLICATION_XML_UTF_8;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static com.google.common.net.MediaType.parse;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.net.HttpURLConnection.HTTP_OK;
import static javax.xml.stream.XMLOutputFactory.newFactory;
import static org.sfs.elasticsearch.container.ListObjects.ListedObject;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_CONTAINER_BYTES_USED;
import static org.sfs.util.SfsHttpHeaders.X_CONTAINER_OBJECT_COUNT;
import static org.sfs.util.SfsHttpQueryParams.FORMAT;
import static org.sfs.vo.ObjectPath.fromPaths;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class GetContainer implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(GetContainer.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {


        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(new LoadAccountAndContainer(vertxContext))
                .flatMap(new ValidateActionContainerListObjects(httpServerRequest))
                .flatMap(persistentContainer -> {

                    Observable<ContainerStats> oContainerStats =
                            just(persistentContainer)
                                    .flatMap(new LoadContainerStats(httpServerRequest.vertxContext()));

                    Observable<ObjectList> oObjectListing =
                            just(persistentContainer)
                                    .flatMap(new ListObjects(httpServerRequest));


                    return combineSinglesDelayError(oContainerStats, oObjectListing, (containerStats, objectList) -> {
                        HttpServerResponse httpServerResponse = httpServerRequest.response();
                        httpServerResponse.setChunked(true);

                        MultiMap headerParams = httpServerRequest.headers();
                        MultiMap queryParams = httpServerRequest.params();
                        String format = queryParams.get(FORMAT);

                        String accept = headerParams.get(ACCEPT);

                        PersistentContainer container = containerStats.getPersistentContainer();

                        Metadata metadata = container.getMetadata();

                        for (String key : metadata.keySet()) {
                            SortedSet<String> values = metadata.get(key);
                            if (values != null && !values.isEmpty()) {
                                httpServerResponse.putHeader(format("%s%s", X_ADD_CONTAINER_META_PREFIX, key), values);
                            }
                        }

                        httpServerResponse.putHeader(X_CONTAINER_OBJECT_COUNT, valueOf(containerStats.getObjectCount()));
                        httpServerResponse.putHeader(
                                X_CONTAINER_BYTES_USED,
                                BigDecimal.valueOf(containerStats.getBytesUsed())
                                        .setScale(0, ROUND_HALF_UP)
                                        .toString()
                        );

                        MediaType parsedAccept = null;

                        if (equalsIgnoreCase("xml", format)) {
                            parsedAccept = APPLICATION_XML_UTF_8;
                        } else if (equalsIgnoreCase("json", format)) {
                            parsedAccept = JSON_UTF_8;
                        }

                        if (parsedAccept == null) {
                            if (!isNullOrEmpty(accept)) {
                                parsedAccept = parse(accept);
                            }
                        }

                        if (parsedAccept == null
                                || (!PLAIN_TEXT_UTF_8.is(parsedAccept)
                                && !APPLICATION_XML_UTF_8.is(parsedAccept)
                                && !JSON_UTF_8.equals(parsedAccept))) {
                            parsedAccept = PLAIN_TEXT_UTF_8;
                        }

                        httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, parsedAccept.toString());
                        httpServerResponse.setStatusCode(HTTP_OK);

                        if (JSON_UTF_8.is(parsedAccept)) {

                            try (OutputStream endableWriteStreamOutputStream = new BufferedOutputStream(new NoCloseOutputStream(new BufferEndableWriteStreamOutputStream(new HttpServerResponseEndableWriteStream(httpServerResponse))), 10485760)) {
                                JsonFactory jsonFactory = vertxContext.verticle().jsonFactory();
                                JsonGenerator jg = jsonFactory.createGenerator(endableWriteStreamOutputStream, UTF8);
                                jg.writeStartArray();

                                for (ListedObject listedObject : ordered(objectList.getObjects())) {

                                    jg.writeStartObject();
                                    jg.writeStringField("hash", base16().lowerCase().encode(listedObject.getEtag()));
                                    jg.writeStringField("last_modified", toDateTimeString(listedObject.getLastModified()));
                                    jg.writeNumberField("bytes", listedObject.getLength());
                                    jg.writeStringField("content_type", listedObject.getContentType());
                                    jg.writeStringField("name", listedObject.getName());
                                    jg.writeEndObject();
                                }

                                jg.writeEndArray();
                                jg.close();
                            } catch (IOException e) {
                                LOGGER.warn(e.getLocalizedMessage(), e);
                            }

                        } else if (APPLICATION_XML_UTF_8.is(parsedAccept)) {

                            String charset = UTF_8.toString();
                            XMLStreamWriter writer = null;
                            try (OutputStream endableWriteStreamOutputStream = new BufferedOutputStream(new NoCloseOutputStream(new BufferEndableWriteStreamOutputStream(new HttpServerResponseEndableWriteStream(httpServerResponse))), 10485760)) {
                                try {
                                    writer = newFactory()
                                            .createXMLStreamWriter(endableWriteStreamOutputStream, charset);

                                    writer.writeStartDocument(charset, "1.0");

                                    writer.writeStartElement("container");

                                    writer.writeAttribute("name", fromPaths(container.getId()).containerName().get());

                                    for (ListedObject listedObject : ordered(objectList.getObjects())) {

                                        writer.writeStartElement("object");

                                        writer.writeStartElement("name");
                                        writer.writeCharacters(listedObject.getName());
                                        writer.writeEndElement();


                                        writer.writeStartElement("hash");
                                        writer.writeCharacters(base16().lowerCase().encode(listedObject.getEtag()));
                                        writer.writeEndElement();


                                        writer.writeStartElement("bytes");
                                        writer.writeCharacters(valueOf(listedObject.getLength()));
                                        writer.writeEndElement();


                                        writer.writeStartElement("content_type");
                                        writer.writeCharacters(listedObject.getContentType());
                                        writer.writeEndElement();


                                        writer.writeStartElement("last_modified");
                                        writer.writeCharacters(toDateTimeString(listedObject.getLastModified()));
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
                            } catch (IOException e) {
                                LOGGER.warn(e.getLocalizedMessage(), e);
                            }

                        } else {
                            String charset = UTF_8.toString();
                            for (ListedObject listedObject : ordered(objectList.getObjects())) {
                                httpServerResponse.write(listedObject.getName(), charset);
                                httpServerResponse.write("\n", charset);
                            }
                        }

                        return (Void) null;
                    });
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                               @Override
                               public void onNext(Void aVoid) {

                               }
                           }

                );

    }

    protected Iterable<ListedObject> ordered(Iterable<ListedObject> iterable) {
        return from(new Comparator<ListedObject>() {
            @Override
            public int compare(ListedObject o1, ListedObject o2) {
                return o1.getName().compareTo(o2.getName());
            }
        }).sortedCopy(iterable);
    }
}