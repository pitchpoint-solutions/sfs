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

package org.sfs;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.sfs.auth.AuthProviderService;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.encryption.AwsKms;
import org.sfs.encryption.AzureKms;
import org.sfs.encryption.ContainerKeys;
import org.sfs.encryption.MasterKeys;
import org.sfs.filesystem.temp.TempDirectoryCleaner;
import org.sfs.jobs.Jobs;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.NodeStats;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.all.elasticsearch.RefreshIndex;
import org.sfs.nodes.all.stats.GetClusterStats;
import org.sfs.nodes.all.stats.GetNodeStats;
import org.sfs.nodes.compute.account.DeleteAccount;
import org.sfs.nodes.compute.account.GetAccount;
import org.sfs.nodes.compute.account.GetAccountMeta;
import org.sfs.nodes.compute.account.HeadAccount;
import org.sfs.nodes.compute.account.PostAccount;
import org.sfs.nodes.compute.container.DeleteOrDestroyContainer;
import org.sfs.nodes.compute.container.ExportContainer;
import org.sfs.nodes.compute.container.GetContainer;
import org.sfs.nodes.compute.container.GetContainerMeta;
import org.sfs.nodes.compute.container.HeadContainer;
import org.sfs.nodes.compute.container.ImportContainer;
import org.sfs.nodes.compute.container.PostContainer;
import org.sfs.nodes.compute.container.PutContainer;
import org.sfs.nodes.compute.container.VerifyRepairAllContainersExecute;
import org.sfs.nodes.compute.container.VerifyRepairAllContainersStop;
import org.sfs.nodes.compute.container.VerifyRepairAllContainersWait;
import org.sfs.nodes.compute.container.VerifyRepairContainerExecute;
import org.sfs.nodes.compute.container.VerifyRepairContainerStop;
import org.sfs.nodes.compute.container.VerifyRepairContainerWait;
import org.sfs.nodes.compute.containerkeys.ReEncryptContainerKeys;
import org.sfs.nodes.compute.identity.PostTokens;
import org.sfs.nodes.compute.masterkey.ReEncryptMasterKeys;
import org.sfs.nodes.compute.masterkey.VerifyRepairMasterKeys;
import org.sfs.nodes.compute.object.DeleteObject;
import org.sfs.nodes.compute.object.GetObject;
import org.sfs.nodes.compute.object.GetObjectMeta;
import org.sfs.nodes.compute.object.HeadObject;
import org.sfs.nodes.compute.object.PostObject;
import org.sfs.nodes.compute.object.PutObject;
import org.sfs.nodes.compute.object.VerifyRepairObjectExecute;
import org.sfs.nodes.compute.object.VerifyRepairObjectStop;
import org.sfs.nodes.compute.test.ResetForTest;
import org.sfs.nodes.compute.test.UpdateClusterStats;
import org.sfs.nodes.data.AckBlob;
import org.sfs.nodes.data.CanReadVolume;
import org.sfs.nodes.data.CanWriteVolume;
import org.sfs.nodes.data.ChecksumBlob;
import org.sfs.nodes.data.DeleteBlob;
import org.sfs.nodes.data.GetBlob;
import org.sfs.nodes.data.PutBlob;
import org.sfs.nodes.master.MasterNodeExecuteJob;
import org.sfs.nodes.master.MasterNodeStopJob;
import org.sfs.nodes.master.MasterNodeWaitForJob;
import org.sfs.rx.Defer;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.Terminus;
import org.sfs.rx.ToVoid;
import org.sfs.thread.NamedCapacityFixedThreadPool;
import org.sfs.util.ConfigHelper;
import org.sfs.util.FileSystemLock;
import rx.Observable;
import rx.functions.Func0;

import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class SfsSingletonServer extends Server implements Shareable {

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private VertxContext<Server> vertxContext;

    private ExecutorService ioPool;
    private ExecutorService backgroundPool;

    private TempDirectoryCleaner tempDirectoryCleaner = new TempDirectoryCleaner();
    private AwsKms awsKms = new AwsKms();
    private AzureKms azureKms = new AzureKms();
    private MasterKeys masterKeys = new MasterKeys();
    private ContainerKeys containerKeys = new ContainerKeys();
    private Elasticsearch elasticsearch = new Elasticsearch();
    private ClusterInfo clusterInfo = new ClusterInfo();
    private NodeStats nodeStats = new NodeStats();
    private Nodes nodes;
    private Jobs jobs = new Jobs();
    private SfsFileSystem sfsFileSystem = new SfsFileSystem();
    private JsonFactory jsonFactory = new JsonFactory();
    private Set<HostAndPort> parsedListenAddresses = new LinkedHashSet<>();
    private Set<HostAndPort> parsedPublishAddresses = new LinkedHashSet<>();
    private Set<HostAndPort> clusterHosts = new LinkedHashSet<>();
    private int httpServerMaxHeaderSize;
    private int httpServerIdleConnectionTimeout;
    private int remoteNodeIdleConnectionTimeout;
    private boolean started = false;
    private Throwable startException = null;
    private int remoteNodeMaxPoolSize;
    private int remoteNodeConnectTimeout;
    private HttpClient httpsClient;
    private HttpClient httpClient;
    private AuthProviderService authProviderService = new AuthProviderService();
    private boolean testMode;
    private byte[] remoteNodeSecret;

    private static final Logger LOGGER = LoggerFactory.getLogger(SfsSingletonServer.class);

    @Override
    public void start(final Future<Void> startedResult) {
        Preconditions.checkState(STARTED.compareAndSet(false, true), "Only one instance is allowed.");
        final SfsSingletonServer _this = this;

        LOGGER.info("Starting verticle " + _this);

        JsonObject config = config();

        String fsHome = ConfigHelper.getFieldOrEnv(config, "fs.home");
        if (fsHome == null) {
            fsHome = System.getProperty("user.home");
            if (fsHome != null) {
                fsHome = Paths.get(fsHome, "data", "sfs").toString();
            }
        }
        config.put("fs.home", fsHome);
        LOGGER.info(String.format("Config: %s", config.encodePrettily()));

        Path fileSystem = Paths.get(fsHome);

        testMode = Boolean.valueOf(ConfigHelper.getFieldOrEnv(config, "test_mode", "false"));

        FileSystemLock fileSystemLock = new FileSystemLock(Paths.get(fileSystem.toString(), ".lock"), 60, TimeUnit.SECONDS);

        for (String jsonListenAddress : ConfigHelper.getArrayFieldOrEnv(config, "http.listen.addresses", new String[]{})) {
            parsedListenAddresses.add(HostAndPort.fromString(jsonListenAddress));
        }

        for (String jsonPublishAddress : ConfigHelper.getArrayFieldOrEnv(config, "http.publish.addresses", Iterables.transform(parsedListenAddresses, input -> input.toString()))) {
            parsedPublishAddresses.add(HostAndPort.fromString(jsonPublishAddress));
        }

        if (parsedPublishAddresses.isEmpty()) {
            parsedPublishAddresses.addAll(parsedListenAddresses);
        }

        for (String jsonClusterHost : ConfigHelper.getArrayFieldOrEnv(config, "cluster.hosts", Iterables.transform(parsedPublishAddresses, input -> input.toString()))) {
            clusterHosts.add(HostAndPort.fromString(jsonClusterHost));
        }

        // adds itself to the list if the list is empty
        if (clusterHosts.isEmpty()) {
            clusterHosts.addAll(parsedPublishAddresses);
        }

        int threadPoolIoQueueSize = new Integer(ConfigHelper.getFieldOrEnv(config, "threadpool.io.queuesize", "10000"));
        Preconditions.checkArgument(threadPoolIoQueueSize > 0, "threadpool.io.queuesize must be greater than 0");

        int threadPoolIoSize = new Integer(ConfigHelper.getFieldOrEnv(config, "threadpool.io.size", String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
        Preconditions.checkArgument(threadPoolIoSize > 0, "threadpool.io.size must be greater than 0");

        int threadPoolBackgroundSize = new Integer(ConfigHelper.getFieldOrEnv(config, "threadpool.background.size", String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
        Preconditions.checkArgument(threadPoolBackgroundSize > 0, "threadpool.background.size must be greater than 0");

        int threadPoolBackgroundQueueSize = new Integer(ConfigHelper.getFieldOrEnv(config, "threadpool.background.queuesize", "10000"));
        Preconditions.checkArgument(threadPoolBackgroundQueueSize > 0, "threadpool.background.queuesize must be greater than 0");

        ioPool = NamedCapacityFixedThreadPool.newInstance(threadPoolIoSize, threadPoolIoQueueSize, "sfs-io-pool");
        backgroundPool = NamedCapacityFixedThreadPool.newInstance(threadPoolBackgroundSize, threadPoolBackgroundQueueSize, "sfs-blocking-action-pool");

        this.vertxContext = new VertxContext<>(this);

        nodes = new Nodes();

        httpServerMaxHeaderSize = new Integer(ConfigHelper.getFieldOrEnv(config, "http.maxheadersize", "8192"));
        Preconditions.checkArgument(httpServerMaxHeaderSize > 0, "http.maxheadersize must be greater than 0");

        httpServerIdleConnectionTimeout = new Integer(ConfigHelper.getFieldOrEnv(config, "http.idleconnectiontimeout", String.valueOf(TimeUnit.MINUTES.toMillis(20))));
        Preconditions.checkArgument(httpServerIdleConnectionTimeout > 0, "http.idleconnectiontimeout must be greater than 0");

        int nodeStatsRefreshInterval = new Integer(ConfigHelper.getFieldOrEnv(config, "node_stats_refresh_interval", String.valueOf(TimeUnit.SECONDS.toMillis(1))));
        Preconditions.checkArgument(nodeStatsRefreshInterval > 0, "node_stats_refresh_interval must be greater than 0");

        remoteNodeIdleConnectionTimeout = new Integer(ConfigHelper.getFieldOrEnv(config, "remotenode.idleconnectiontimeout", "30"));
        Preconditions.checkArgument(remoteNodeIdleConnectionTimeout > 0, "remotenode.idleconnectiontimeout must be greater than 0");

        remoteNodeMaxPoolSize = new Integer(ConfigHelper.getFieldOrEnv(config, "remotenode.maxpoolsize", "25"));
        Preconditions.checkArgument(remoteNodeMaxPoolSize > 0, "remotenode.maxpoolsize must be greater than 0");

        remoteNodeConnectTimeout = new Integer(ConfigHelper.getFieldOrEnv(config, "remotenode.connectimeout", "30000"));
        Preconditions.checkArgument(remoteNodeConnectTimeout > 0, "remotenode.connectimeout must be greater than 0");

        int remoteNodeResponseTimeout = new Integer(ConfigHelper.getFieldOrEnv(config, "remotenode.responsetimeout", "30000"));
        Preconditions.checkArgument(remoteNodeResponseTimeout > 0, "remotenode.responsetimeout must be greater than 0");

        String strRemoteNodeSecret = ConfigHelper.getFieldOrEnv(config, "remotenode.secret");
        Preconditions.checkArgument(strRemoteNodeSecret != null, "remotenode.secret is required");
        remoteNodeSecret = BaseEncoding.base64().decode(strRemoteNodeSecret);

        int numberOfObjectReplicas = new Integer(ConfigHelper.getFieldOrEnv(config, "number_of_object_replicas", "0"));
        Preconditions.checkArgument(numberOfObjectReplicas >= 0, "number_of_object_replicas must be greater or equal to 0");

        int tempFileTtl = new Integer(ConfigHelper.getFieldOrEnv(config, "temp_file_ttl", "86400000"));
        Preconditions.checkArgument(tempFileTtl >= 0, "temp_file_ttl must be greater or equal to 0");

        final boolean dataNode = Boolean.valueOf(ConfigHelper.getFieldOrEnv(config, "node.data", "true"));
        final boolean masterNode = Boolean.valueOf(ConfigHelper.getFieldOrEnv(config, "node.master", "true"));

        this.httpsClient = createHttpClient(vertx, true);
        this.httpClient = createHttpClient(vertx, false);

        Defer.aVoid()
                .flatMap(aVoid -> sfsFileSystem.open(vertxContext, fileSystem))
                .flatMap(aVoid -> fileSystemLock.lock(vertxContext))
                .flatMap(aVoid -> authProviderService.open(vertxContext))
                .flatMap(aVoid -> awsKms.start(vertxContext, config))
                .flatMap(aVoid -> azureKms.start(vertxContext, config))
                .flatMap(aVoid -> tempDirectoryCleaner.start(vertxContext, tempFileTtl))
                .flatMap(aVoid -> elasticsearch.start(vertxContext, config, masterNode))
                .flatMap(aVoid ->
                        nodes.open(vertxContext,
                                parsedPublishAddresses,
                                clusterHosts,
                                remoteNodeMaxPoolSize,
                                remoteNodeConnectTimeout,
                                remoteNodeResponseTimeout,
                                numberOfObjectReplicas,
                                nodeStatsRefreshInterval,
                                dataNode,
                                masterNode))
                .flatMap(aVoid -> nodeStats.open(vertxContext))
                .flatMap(aVoid -> clusterInfo.open(vertxContext))
                .flatMap(aVoid -> masterKeys.start(vertxContext))
                .flatMap(aVoid -> containerKeys.start(vertxContext))
                .flatMap(aVoid -> jobs.open(vertxContext, config))
                .flatMap(aVoid -> initHttpListeners(vertxContext))
                .flatMap(aVoid -> {
                    // make httpclient bind to correct context
                    HostAndPort firstPublishedAddress = nodes.getHostAndPort();
                    String url =
                            format("http://%s/admin/001/healthcheck", firstPublishedAddress);
                    Func0<Observable<Void>> func0 = () -> {
                        LOGGER.debug("Trying to connect to health check {}", url);
                        ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
                        HttpClientRequest httpClientRequest = httpClient.getAbs(url, httpClientResponse -> {
                            httpClientResponse.pause();
                            handler.complete(httpClientResponse);
                        }).exceptionHandler(handler::fail)
                                .setTimeout(5000);
                        httpClientRequest.end();
                        return handler.flatMap(new HttpClientResponseBodyBuffer())
                                .map(new ToVoid<>());
                    };
                    return RxHelper.onErrorResumeNext(20, func0);
                })
                .subscribe(
                        o -> {
                            // do nothing
                        }, throwable -> {
                            LOGGER.error("Failed to start verticle " + _this, throwable);
                            startException = throwable;
                            started = true;
                            SfsServer.setDelegate(vertx, _this);
                            startedResult.fail(throwable);
                        }, () -> {
                            LOGGER.info("Started verticle " + _this);
                            started = true;
                            SfsServer.setDelegate(vertx, _this);
                            startedResult.complete();
                        });
    }

    @Override
    public void stop(final Future<Void> stoppedResult) {
        final SfsSingletonServer _this = this;
        LOGGER.info("Stopping verticle " + _this);

        Defer.aVoid()
                .flatMap(aVoid -> {
                    if (jobs != null) {
                        return jobs
                                .close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    }
                    return Defer.aVoid();
                })
                .flatMap(aVoid -> {
                    if (tempDirectoryCleaner != null) {
                        return tempDirectoryCleaner
                                .stop()
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    }
                    return Defer.aVoid();
                })
                .flatMap(aVoid -> {
                    if (containerKeys != null) {
                        return containerKeys
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (masterKeys != null) {
                        return masterKeys
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (clusterInfo != null) {
                        return clusterInfo.close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (nodeStats != null) {
                        return nodeStats.close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (nodes != null) {
                        return nodes
                                .close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (elasticsearch != null) {
                        return elasticsearch
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    } else {
                        return Defer.aVoid();
                    }
                })
                .flatMap(aVoid -> {
                    if (azureKms != null) {
                        return azureKms
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    }
                    return Defer.aVoid();
                })
                .flatMap(aVoid -> {
                    if (awsKms != null) {
                        return awsKms
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    }
                    return Defer.aVoid();
                })
                .flatMap(aVoid -> {
                    if (authProviderService != null) {
                        return authProviderService
                                .close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.aVoid();
                                });
                    }
                    return Defer.aVoid();
                })
                .doOnNext(aVoid -> {
                    if (ioPool != null) {
                        ioPool.shutdown();
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("Unhandled Exception", throwable);
                    return Defer.aVoid();
                })
                .doOnNext(aVoid -> {
                    if (backgroundPool != null) {
                        backgroundPool.shutdown();
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("Unhandled Exception", throwable);
                    return Defer.aVoid();
                })
                .doOnNext(aVoid -> {
                    for (String envVar : ConfigHelper.getEnvVars()) {
                        System.out.println("ENV " + envVar);
                    }
                })
                .subscribe(
                        aVoid -> {
                            // do nothing
                        },
                        throwable -> {
                            STARTED.set(false);
                            LOGGER.info("Failed to stop verticle " + _this, throwable);
                            SfsServer.removeDelegate(vertx, _this);
                            stoppedResult.fail(throwable);
                        },
                        () -> {
                            STARTED.set(false);
                            LOGGER.info("Stopped verticle " + _this);
                            SfsServer.removeDelegate(vertx, _this);
                            stoppedResult.complete();
                        });

    }

    @Override
    public ExecutorService getIoPool() {
        return ioPool;
    }

    @Override
    public ExecutorService getBackgroundPool() {
        return backgroundPool;
    }

    @Override
    public VertxContext<Server> vertxContext() {
        return vertxContext;
    }

    public boolean isStarted() {
        return started;
    }

    public Throwable getStartException() {
        return startException;
    }

    @Override
    public AuthProviderService authProviderService() {
        return authProviderService;
    }

    @Override
    public TempDirectoryCleaner tempFileFactory() {
        return tempDirectoryCleaner;
    }

    @Override
    public Elasticsearch elasticsearch() {
        return elasticsearch;
    }

    @Override
    public Nodes nodes() {
        return nodes;
    }

    @Override
    public Jobs jobs() {
        return jobs;
    }

    @Override
    public SfsFileSystem sfsFileSystem() {
        return sfsFileSystem;
    }

    @Override
    public JsonFactory jsonFactory() {
        return jsonFactory;
    }

    @Override
    public AwsKms awsKms() {
        return awsKms;
    }

    @Override
    public AzureKms azureKms() {
        return azureKms;
    }

    @Override
    public MasterKeys masterKeys() {
        return masterKeys;
    }

    @Override
    public ContainerKeys containerKeys() {
        return containerKeys;
    }

    @Override
    public HttpClient httpClient(boolean https) {
        return https ? httpsClient : httpClient;
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public NodeStats getNodeStats() {
        return nodeStats;
    }

    @Override
    public byte[] getRemoteNodeSecret() {
        return remoteNodeSecret;
    }

    @Override
    public Context getContext() {
        return context;
    }

    public Observable<List<HttpServer>> initHttpListeners(VertxContext<Server> vertxContext) {
        return Observable.from(parsedListenAddresses)
                .flatMap(hostAndPort -> createHttpServer(vertxContext, hostAndPort, createRouter(vertxContext)))
                .toList();
    }

    protected Observable<HttpServer> createHttpServer(VertxContext<Server> vertxContext, HostAndPort hostAndPort, Router router) {
        ObservableFuture<HttpServer> handler = RxHelper.observableFuture();
        HttpServerOptions httpServerOptions = new HttpServerOptions()
                .setMaxHeaderSize(httpServerMaxHeaderSize)
                .setCompressionSupported(false)
                .setAcceptBacklog(10000)
                .setReuseAddress(true)
                .setIdleTimeout(httpServerIdleConnectionTimeout)
                .setHandle100ContinueAutomatically(true);
        vertxContext.vertx().createHttpServer(httpServerOptions)
                .requestHandler(router::accept)
                .listen(hostAndPort.getPort(), hostAndPort.getHostText(), handler.toHandler());
        return handler;
    }

    protected HttpClient createHttpClient(Vertx v, boolean https) {
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(remoteNodeConnectTimeout)
                .setMaxPoolSize(remoteNodeMaxPoolSize)
                .setKeepAlive(true)
                .setPipelining(false)
                .setMaxWaitQueueSize(200)
                .setReuseAddress(true)
                .setIdleTimeout(remoteNodeIdleConnectionTimeout)
                .setSsl(https);

        HttpClient client = v.createHttpClient(httpClientOptions);

        return client;
    }

    protected Router createRouter(VertxContext<Server> vertxContext) {
        Vertx vertx = vertxContext.vertx();

        Router router = Router.router(vertx);
        router.routeWithRegex("\\/sfs\\/(.*)").handler(routingContext -> {
            // this is here if you decide to run with a context root and a proxy won't
            // remove the context root
            routingContext.request().headers().add("X-Context-Root", "sfs");
            String noContextRoutePath = routingContext.request().getParam("param0");
            routingContext.reroute("/" + noContextRoutePath);
        });

        router.get("/admin/001/healthcheck").handler(httpServerRequest -> httpServerRequest
                .response()
                .setStatusCode(HttpURLConnection.HTTP_OK)
                .end());

        // cluster stats
        router.get("/stats").handler(new SfsRequestHandler(vertxContext, new GetClusterStats()));


        // object admin method
        router.get("/metadata_accounts/*").handler(new SfsRequestHandler(vertxContext, new GetAccountMeta()));
        router.get("/metadata_containers/*").handler(new SfsRequestHandler(vertxContext, new GetContainerMeta()));
        router.get("/metadata_objects/*").handler(new SfsRequestHandler(vertxContext, new GetObjectMeta()));

        //re encrypt
        router.post("/reencrypt_masterkeys").handler(new SfsRequestHandler(vertxContext, new ReEncryptMasterKeys()));
        router.post("/reencrypt_containerkeys").handler(new SfsRequestHandler(vertxContext, new ReEncryptContainerKeys()));

        //repair
        router.post("/verify_repair_masterkeys").handler(new SfsRequestHandler(vertxContext, new VerifyRepairMasterKeys()));
        router.postWithRegex("\\/verify_repair_objects\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairObjectExecute()));
        router.deleteWithRegex("\\/verify_repair_objects\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairObjectStop()));
        router.getWithRegex("\\/verify_repair_objects\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairObjectStop()));

        router.postWithRegex("\\/verify_repair_containers\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairContainerExecute()));
        router.deleteWithRegex("\\/verify_repair_containers\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairContainerStop()));
        router.getWithRegex("\\/verify_repair_containers\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new VerifyRepairContainerWait()));

        router.post("/verify_repair_containers").handler(new SfsRequestHandler(vertxContext, new VerifyRepairAllContainersExecute()));
        router.delete("/verify_repair_containers").handler(new SfsRequestHandler(vertxContext, new VerifyRepairAllContainersStop()));
        router.get("/verify_repair_containers").handler(new SfsRequestHandler(vertxContext, new VerifyRepairAllContainersWait()));


        // container admin method
        router.post("/export_container/*").handler(new SfsRequestHandler(vertxContext, new ExportContainer()));
        router.post("/import_container/*").handler(new SfsRequestHandler(vertxContext, new ImportContainer()));

        // openstack keystone
        router.post("/v2.0/tokens").handler(new SfsRequestHandler(vertxContext, new PostTokens()));


        // openstack swift object methods
        router.getWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new GetObject()));
        router.headWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new HeadObject()));
        router.deleteWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new DeleteObject()));
        router.putWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new PutObject()));
        router.postWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new PostObject()));


        // openstack swift container methods
        router.getWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new GetContainer()));
        router.headWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new HeadContainer()));
        router.putWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new PutContainer()));
        router.postWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new PostContainer()));
        router.deleteWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new DeleteOrDestroyContainer()));


        // openstack swift account methods
        router.getWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new GetAccount()));
        router.headWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new HeadAccount()));
        router.postWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new PostAccount()));
        router.deleteWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new DeleteAccount()));


        // node specific calls below. ie. A call to any node that requires a master will get routed to the master node

        // commands that must run on the master node
        router.post("/_internal_node_master_execute_job").handler(new SfsRequestHandler(vertxContext, new MasterNodeExecuteJob()));
        router.post("/_internal_node_master_wait_for_job").handler(new SfsRequestHandler(vertxContext, new MasterNodeWaitForJob()));
        router.post("/_internal_node_master_stop_job").handler(new SfsRequestHandler(vertxContext, new MasterNodeStopJob()));

        // node stats methods
        router.get("/_internal_node/stats").handler(new SfsRequestHandler(vertxContext, new GetNodeStats()));

        // data node blob store methods
        router.delete("/_internal_node_data/blob").handler(new SfsRequestHandler(vertxContext, new DeleteBlob()));
        router.get("/_internal_node_data/blob").handler(new SfsRequestHandler(vertxContext, new GetBlob()));
        router.put("/_internal_node_data/blob").handler(new SfsRequestHandler(vertxContext, new PutBlob()));
        router.get("/_internal_node_data/blob/canwrite").handler(new SfsRequestHandler(vertxContext, new CanWriteVolume()));
        router.get("/_internal_node_data/blob/canread").handler(new SfsRequestHandler(vertxContext, new CanReadVolume()));
        router.put("/_internal_node_data/blob/ack").handler(new SfsRequestHandler(vertxContext, new AckBlob()));
        router.get("/_internal_node_data/blob/checksum").handler(new SfsRequestHandler(vertxContext, new ChecksumBlob()));

        if (testMode) {
            router.post("/admin/001/resetfortest").handler(new SfsRequestHandler(vertxContext, new ResetForTest()));
            router.post("/admin/001/updateclusterstats").handler(new SfsRequestHandler(vertxContext, new UpdateClusterStats()));
            router.post("/admin/001/refresh_index").handler(new SfsRequestHandler(vertxContext, new RefreshIndex()));
            router.get("/admin/001/is_online").handler(new SfsRequestHandler(vertxContext,
                    sfsRequest ->
                            Defer.aVoid()
                                    .flatMap(aVoid1 -> sfsRequest.vertxContext().verticle().getClusterInfo().isOnline())
                                    .doOnNext(isOnline -> {
                                        if (Boolean.TRUE.equals(isOnline)) {
                                            sfsRequest.response().setStatusCode(HttpURLConnection.HTTP_OK);
                                        } else {
                                            sfsRequest.response().setStatusCode(HttpURLConnection.HTTP_NO_CONTENT);
                                        }
                                    })
                                    .subscribe(new Terminus<Boolean>(sfsRequest) {
                                        @Override
                                        public void onNext(Boolean aBoolean) {

                                        }
                                    })));
        }

        return router;
    }

    protected static class SfsRequestHandler implements Handler<RoutingContext> {

        private final VertxContext<Server> vertxContext;
        private final Handler<SfsRequest> delegate;

        public SfsRequestHandler(VertxContext<Server> vertxContext, Handler<SfsRequest> delegate) {
            this.vertxContext = vertxContext;
            this.delegate = delegate;
        }

        @Override
        public void handle(RoutingContext routingContext) {
            routingContext.addBodyEndHandler(event -> {
                HttpServerRequest request = routingContext.request();
                try {
                    request.resume();
                } catch (Throwable e) {
                    // do nothing
                }
            });
            SfsRequest sfsRequest = new SfsRequest(vertxContext, routingContext.request());
            delegate.handle(sfsRequest);
        }
    }

}

