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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.io.CharStreams;
import com.google.common.net.HostAndPort;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.slf4j.Slf4jESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.Defer;
import org.sfs.rx.ResultMemoizeHandler;
import org.sfs.rx.ToVoid;
import org.sfs.util.ExceptionHelper;
import org.sfs.util.JsonHelper;
import org.sfs.util.Limits;
import rx.Observable;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Elasticsearch {

    enum Status {
        STARTING,
        STOPPING,
        STARTED,
        STOPPED
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Elasticsearch.class);
    private Client elasticSearchClient;
    private long defaultIndexTimeout;
    private long defaultSearchTimeout;
    private long defaultDeleteTimeout;
    private long defaultAdminTimeout;
    private long defaultGetTimeout;
    private long defaultScrollTimeout;
    private int shards;
    private int replicas;
    private AtomicReference<Status> status = new AtomicReference<>(Status.STOPPED);

    public Elasticsearch() {
    }

    public Observable<Void> start(final VertxContext<Server> vertxContext, final JsonObject config) {
        return Defer.empty()
                .filter(aVoid -> status.compareAndSet(Status.STOPPED, Status.STARTING))
                .flatMap(aVoid -> vertxContext.executeBlocking(
                        () -> {
                            if (elasticSearchClient == null) {
                                LOGGER.debug("Starting Elasticsearch");
                                try {
                                    ESLoggerFactory.setDefaultFactory(new Slf4jESLoggerFactory());

                                    defaultScrollTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultscrolltimeout", String.valueOf(TimeUnit.MINUTES.toMillis(2))));
                                    defaultIndexTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultindextimeout", "500"));
                                    defaultGetTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultgettimeout", "500"));
                                    defaultSearchTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultsearchtimeout", String.valueOf(TimeUnit.SECONDS.toMillis(5))));
                                    defaultDeleteTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultdeletetimeout", "500"));
                                    defaultAdminTimeout = Long.parseLong(JsonHelper.getField(config, "elasticsearch.defaultadmintimeout", String.valueOf(TimeUnit.SECONDS.toMillis(30))));
                                    shards = Integer.parseInt(JsonHelper.getField(config, "elasticsearch.shards", String.valueOf(1)));
                                    replicas = Integer.parseInt(JsonHelper.getField(config, "elasticsearch.replicas", String.valueOf(0)));

                                    Settings.Builder settings = Settings.settingsBuilder();
                                    settings.put("node.client", true);
                                    String clusterName = JsonHelper.getField(config, "elasticsearch.cluster.name");
                                    if (clusterName != null) {
                                        settings.put("cluster.name", clusterName);
                                    }
                                    String nodeName = JsonHelper.getField(config, "elasticsearch.node.name");
                                    if (nodeName != null) {
                                        settings.put("node.name", nodeName);
                                    }
                                    JsonArray unicastHosts = config.getJsonArray("elasticsearch.discovery.zen.ping.unicast.hosts", new JsonArray());
                                    settings.put("discovery.zen.ping.multicast.enabled", JsonHelper.getField(config, "elasticsearch.discovery.zen.ping.multicast.enabled", "true"));
                                    settings.put("discovery.zen.ping.unicast.enabled", JsonHelper.getField(config, "elasticsearch.discovery.zen.ping.unicast.enabled", "false"));
                                    settings.put("discovery.zen.ping.unicast.hosts", Joiner.on(',').join(config.getJsonArray("elasticsearch.discovery.zen.ping.unicast.hosts", new JsonArray())));
                                    settings.put("client.transport.sniff", "true");
                                    Iterable<InetSocketTransportAddress> transports =
                                            FluentIterable.from(unicastHosts)
                                                    .transform(input -> (String) input)
                                                    .filter(Predicates.notNull())
                                                    .transform(HostAndPort::fromString)
                                                    .transform(input -> {
                                                        try {
                                                            return new InetSocketTransportAddress(InetAddress.getByName(input.getHostText()), input.getPortOrDefault(9300));
                                                        } catch (UnknownHostException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    });


                                    TransportClient transportClient = TransportClient.builder().settings(settings).build();
                                    for (InetSocketTransportAddress transportAddress : transports) {
                                        transportClient.addTransportAddress(transportAddress);
                                    }
                                    elasticSearchClient = transportClient;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            return null;
                        }))
                .flatMap(aVoid -> prepareCommonIndex(vertxContext))
                .doOnNext(aVoid -> Preconditions.checkState(status.compareAndSet(Status.STARTING, Status.STARTED)))
                .doOnNext(aVoid -> LOGGER.debug("Started Elasticsearch"));
    }

    public long getDefaultScrollTimeout() {
        return defaultScrollTimeout;
    }

    public long getDefaultGetTimeout() {
        return defaultGetTimeout;
    }

    public long getDefaultAdminTimeout() {
        return defaultAdminTimeout;
    }

    public long getDefaultDeleteTimeout() {
        return defaultDeleteTimeout;
    }

    public long getDefaultIndexTimeout() {
        return defaultIndexTimeout;
    }

    public long getDefaultSearchTimeout() {
        return defaultSearchTimeout;
    }

    public String defaultType() {
        return "default";
    }

    public String indexPrefix() {
        return "sfs_v0_";
    }

    public Observable<Void> prepareCommonIndex(VertxContext<Server> vertxContext) {
        return createUpdateIndex(vertxContext, serviceDefTypeIndex(), "es-service-def-mapping.json", Limits.NOT_SET, Limits.NOT_SET)
                .flatMap(aVoid -> createUpdateIndex(vertxContext, accountIndex(), "es-account-mapping.json", Limits.NOT_SET, Limits.NOT_SET))
                .flatMap(aVoid -> createUpdateIndex(vertxContext, containerIndex(), "es-container-mapping.json", Limits.NOT_SET, Limits.NOT_SET))
                .flatMap(aVoid -> createUpdateIndex(vertxContext, containerKeyIndex(), "es-container-key-mapping.json", Limits.NOT_SET, Limits.NOT_SET))
                .flatMap(aVoid -> createUpdateIndex(vertxContext, masterKeyTypeIndex(), "es-master-key-mapping.json", Limits.NOT_SET, Limits.NOT_SET));
    }

    public Observable<Void> prepareObjectIndex(VertxContext<Server> vertxContext, String containerName, int shards, int replicas) {
        Preconditions.checkState(shards == Limits.NOT_SET || shards >= 1, "Shards must be >= 1");
        Preconditions.checkState(replicas == Limits.NOT_SET || replicas >= 0, "Replicas must be >= 0");
        String objectIndexName = objectIndex(containerName);
        return createUpdateIndex(vertxContext, objectIndexName, "es-object-mapping.json", shards, replicas);
    }

    public Observable<Void> deleteObjectIndex(VertxContext<Server> vertxContext, String containerName) {
        String objectIndexName = objectIndex(containerName);
        return deleteIndex(vertxContext, objectIndexName);
    }

    protected Observable<Void> deleteIndex(VertxContext<Server> vertxContext, String index) {
        return Defer.just(index)
                .flatMap(new IndexDelete(vertxContext))
                .doOnNext(success -> Preconditions.checkState(success, "Failed to delete index %s", index))
                .map(new ToVoid<>())
                .onErrorResumeNext(throwable -> {
                    if (ExceptionHelper.containsException(IndexNotFoundException.class, throwable)) {
                        return Defer.empty();
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    protected Observable<Void> createUpdateIndex(VertxContext<Server> vertxContext, String index, String mapping, int shards, int replicas) {
        Elasticsearch _this = this;
        return getMapping(vertxContext, mapping)
                .flatMap(mappingData ->
                        Defer.just(index)
                                .flatMap(new IndexExists(vertxContext))
                                .flatMap(exists -> {
                                    if (Boolean.TRUE.equals(exists)) {
                                        return Defer.just(index)
                                                .flatMap(new IndexUpdateMapping(vertxContext, defaultType(), mappingData))
                                                .doOnNext(success -> Preconditions.checkState(success, "Failed to updated index mapping %s", index))
                                                .map(new ToVoid<>())
                                                .flatMap(aVoid -> {
                                                    if (replicas >= 0) {
                                                        Settings settings =
                                                                Settings.settingsBuilder()
                                                                        .put("index.number_of_replicas", replicas)
                                                                        .build();
                                                        IndexUpdateSettings indexUpdateSettings = new IndexUpdateSettings(vertxContext, settings);
                                                        return indexUpdateSettings.call(index);
                                                    } else {
                                                        return Defer.just(true);
                                                    }
                                                })
                                                .doOnNext(success -> Preconditions.checkState(success, "Failed to updated index settings %s", index))
                                                .map(success -> index);
                                    } else {
                                        return Defer.just(index)
                                                .flatMap(new IndexCreate(vertxContext)
                                                        .withMapping(defaultType(), mappingData)
                                                        .setSettings(Settings.settingsBuilder()
                                                                .put("index.refresh_interval", "1s")
                                                                .put("index.number_of_replicas", replicas >= 0 ? replicas : _this.replicas)
                                                                .put("index.number_of_shards", shards >= 1 ? shards : _this.shards)
                                                                .build()))
                                                .doOnNext(success -> Preconditions.checkState(success, "Failed to create index %s", index))
                                                .map(success -> index);
                                    }
                                }))
                .map(new ToVoid<>())
                .flatMap(new IndexWaitForStatus(vertxContext, index, ClusterHealthStatus.GREEN));
    }

    protected Observable<String> getMapping(VertxContext<Server> vertxContext, final String name) {
        return vertxContext.executeBlocking(() -> {
            try (Reader reader = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(name), Charsets.UTF_8)) {
                return CharStreams.toString(reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public String accountIndex() {
        return indexPrefix() + "account";
    }

    public String containerIndex() {
        return indexPrefix() + "container";
    }

    public String objectIndex(String containerName) {
        return String.format("%s%s_objects", indexPrefix(), containerName);
    }

    public String serviceDefTypeIndex() {
        return indexPrefix() + "service_def";
    }

    public String containerKeyIndex() {
        return indexPrefix() + "container_key";
    }

    public String masterKeyTypeIndex() {
        return indexPrefix() + "master_key";
    }

    public boolean isObjectIndex(String indexName) {
        return indexName != null && indexName.startsWith(indexPrefix()) && indexName.endsWith("_objects");
    }

    public Client get() {
        return elasticSearchClient;
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return Defer.empty()
                .filter(aVoid -> status.compareAndSet(Status.STARTED, Status.STOPPING) || status.compareAndSet(Status.STARTING, Status.STOPPING))
                .flatMap(aVoid -> vertxContext.executeBlocking(() -> {
                    LOGGER.debug("Stopping Elasticsearch");
                    if (elasticSearchClient != null) {
                        try {
                            elasticSearchClient.close();
                        } catch (Throwable e) {
                            LOGGER.warn(e.getLocalizedMessage(), e);
                        }
                        elasticSearchClient = null;
                    }
                    LOGGER.debug("Stopped Elasticsearch");
                    return (Void) null;
                }))
                .doOnNext(aVoid -> Preconditions.checkState(status.compareAndSet(Status.STOPPING, Status.STOPPED)));

    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Observable<Optional<Response>> execute(VertxContext<Server> vertxContext, final RequestBuilder actionRequestBuilder, long timeoutMs) {
        return execute(vertxContext.vertx(), actionRequestBuilder, timeoutMs);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> Observable<Optional<Response>> execute(Vertx vertx, final RequestBuilder actionRequestBuilder, long timeoutMs) {
        Context context = vertx.getOrCreateContext();
        ResultMemoizeHandler<Response> handler = new ResultMemoizeHandler<>();
        actionRequestBuilder.execute(new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                context.runOnContext(event -> handler.complete(response));
            }

            @Override
            public void onFailure(Throwable e) {
                context.runOnContext(event -> handler.fail(e));
            }
        });
        return Observable.create(handler.subscribe)
                .doOnNext(response -> {
                    if (response instanceof SearchResponse) {
                        SearchResponse searchResponse = (SearchResponse) response;
                        int totalShards = searchResponse.getTotalShards();
                        int successfulShards = searchResponse.getSuccessfulShards();
                        Preconditions.checkState(totalShards == successfulShards, "%s shards succeeded, expected %s", successfulShards, totalShards);
                    } else if (response instanceof ActionWriteResponse) {
                        ActionWriteResponse actionWriteResponse = (ActionWriteResponse) response;
                        ActionWriteResponse.ShardInfo shardInfo = actionWriteResponse.getShardInfo();
                        int totalShards = shardInfo.getTotal();
                        int successfulShards = shardInfo.getSuccessful();
                        Preconditions.checkState(totalShards == successfulShards, "%s shards succeeded, expected %s", successfulShards, totalShards);
                    } else if (response instanceof AcknowledgedResponse) {
                        AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) response;
                        Preconditions.checkState(acknowledgedResponse.isAcknowledged(), "request not ack'd");
                    }
                })
                .map(Optional::of)
                .onErrorResumeNext(throwable -> {
                    if (ExceptionHelper.containsException(DocumentAlreadyExistsException.class, throwable)
                            || ExceptionHelper.containsException(VersionConflictEngineException.class, throwable)) {
                        return Observable.just(Optional.absent());
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

}
