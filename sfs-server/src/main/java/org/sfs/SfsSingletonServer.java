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
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
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
import org.sfs.nodes.Nodes;
import org.sfs.nodes.all.DumpNode;
import org.sfs.nodes.all.elasticsearch.RefreshIndex;
import org.sfs.nodes.all.masterkey.MasterKeysCheck;
import org.sfs.nodes.all.stats.GetClusterStats;
import org.sfs.nodes.compute.account.DeleteAccount;
import org.sfs.nodes.compute.account.GetAccount;
import org.sfs.nodes.compute.account.HeadAccount;
import org.sfs.nodes.compute.account.PostAccount;
import org.sfs.nodes.compute.container.DeleteContainer;
import org.sfs.nodes.compute.container.ExportContainer;
import org.sfs.nodes.compute.container.GetContainer;
import org.sfs.nodes.compute.container.HeadContainer;
import org.sfs.nodes.compute.container.ImportContainer;
import org.sfs.nodes.compute.container.PostContainer;
import org.sfs.nodes.compute.container.PutContainer;
import org.sfs.nodes.compute.identity.PostTokens;
import org.sfs.nodes.compute.object.DeleteObject;
import org.sfs.nodes.compute.object.GetObject;
import org.sfs.nodes.compute.object.GetObjectMeta;
import org.sfs.nodes.compute.object.HeadObject;
import org.sfs.nodes.compute.object.PostObject;
import org.sfs.nodes.compute.object.PutObject;
import org.sfs.nodes.compute.object.RepairObject;
import org.sfs.nodes.data.AckBlob;
import org.sfs.nodes.data.CanPutBlob;
import org.sfs.nodes.data.ChecksumBlob;
import org.sfs.nodes.data.DeleteBlob;
import org.sfs.nodes.data.GetBlob;
import org.sfs.nodes.data.PutBlob;
import org.sfs.nodes.master.RunNodeJobs;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.Defer;
import org.sfs.rx.RxHelper;
import org.sfs.rx.Terminus;
import org.sfs.rx.WaitForEmptyQueue;
import org.sfs.thread.NamedThreadFactory;
import org.sfs.util.FileSystemLock;
import org.sfs.util.JsonHelper;
import org.sfs.vo.TransientXListener;
import rx.Observable;

import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SfsSingletonServer extends Server implements Shareable {

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private VertxContext<Server> vertxContext;

    private BlockingQueue<Runnable> ioQueue;
    private BlockingQueue<Runnable> backgroundQueue;
    private ExecutorService ioPool;
    private ExecutorService backgroundPool;

    private AtomicReference<Router> activeRouter = new AtomicReference<>();
    private TempDirectoryCleaner tempDirectoryCleaner = new TempDirectoryCleaner();
    private AwsKms awsKms = new AwsKms();
    private AzureKms azureKms = new AzureKms();
    private MasterKeys masterKeys = new MasterKeys();
    private ContainerKeys containerKeys = new ContainerKeys();
    private Elasticsearch elasticsearch = new Elasticsearch();
    private Nodes nodes;
    private Jobs jobs = new Jobs();
    private SfsFileSystem sfsFileSystem = new SfsFileSystem();
    private JsonFactory jsonFactory = new JsonFactory();
    private List<HostAndPort> parsedListenAddresses;
    private List<HostAndPort> parsedPublishAddresses;
    private int verticleMaxHeaderSize;
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
        final Server _this = this;

        LOGGER.info("Starting verticle " + _this);

        JsonObject config = config();

        String fsHome = config.getString("fs.home");
        if (fsHome == null) {
            fsHome = System.getProperty("user.home");
            if (fsHome != null) {
                fsHome = Paths.get(fsHome, "data", "sfs").toString();
            }
        }
        config.put("fs.home", fsHome);
        LOGGER.info(String.format("Config: %s", config.encodePrettily()));

        Path fileSystem = Paths.get(fsHome);
        LOGGER.info(String.format("File System Root: %s", fileSystem));

        testMode = Boolean.valueOf(JsonHelper.getField(config, "test_mode", "false"));
        LOGGER.info(String.format("test_mode: %b", testMode));

        FileSystemLock fileSystemLock = new FileSystemLock(Paths.get(fileSystem.toString(), ".lock"), 60, TimeUnit.SECONDS);



        String[] listenAddresses =
                Iterables.toArray(Splitter.on(',')
                                .omitEmptyStrings()
                                .trimResults()
                                .split(JsonHelper.getField(config, "http.listen.addresses", "0.0.0.0")),
                        String.class);

        String[] publishAddresses =
                Iterables.toArray(Splitter.on(',')
                                .omitEmptyStrings()
                                .trimResults()
                                .split(JsonHelper.getField(config, "http.publish.addresses", "0.0.0.0")),
                        String.class);

        if (publishAddresses.length <= 0) {
            publishAddresses = listenAddresses;
        }

        String listenPorts = JsonHelper.getField(config, "http.listen.port", "8090-9090");
        String[] ports =
                Iterables.toArray(Splitter.on('-')
                        .omitEmptyStrings()
                        .trimResults()
                        .split(listenPorts), String.class);

        parsedListenAddresses = new ArrayList<>();
        parsedPublishAddresses = new ArrayList<>();
        if (ports.length == 1) {
            int parsed = Integer.parseInt(ports[0]);
            for (String listenAddress : listenAddresses) {
                parsedListenAddresses.add(HostAndPort.fromParts(listenAddress, parsed));
            }
            for (String publishAddress : publishAddresses) {
                parsedPublishAddresses.add(HostAndPort.fromParts(publishAddress, parsed));
            }
        } else if (ports.length == 2) {
            int first = Integer.parseInt(ports[0]);
            int last = Integer.parseInt(ports[1]);
            for (String listenAddress : listenAddresses) {
                for (int i = first; i <= last; i++) {
                    HostAndPort hostAndPort = HostAndPort.fromParts(listenAddress, i);
                    parsedListenAddresses.add(hostAndPort);
                }
            }
            for (String publishAddress : publishAddresses) {
                for (int i = first; i <= last; i++) {
                    HostAndPort hostAndPort = HostAndPort.fromParts(publishAddress, i);
                    parsedPublishAddresses.add(hostAndPort);
                }
            }
        }

        int threadPoolIoQueueSize = new Integer(JsonHelper.getField(config, "threadpool.io.queuesize", "10000"));
        LOGGER.info(String.format("threadpool.io.queuesize: %d", threadPoolIoQueueSize));
        Preconditions.checkArgument(threadPoolIoQueueSize > 0, "threadpool.io.queuesize must be greater than 0");

        int threadPoolIoSize = new Integer(JsonHelper.getField(config, "threadpool.io.size", String.valueOf(Runtime.getRuntime().availableProcessors() * 2)));
        LOGGER.info(String.format("threadpool.io.size: %d", threadPoolIoSize));
        Preconditions.checkArgument(threadPoolIoSize > 0, "threadpool.io.size must be greater than 0");

        int threadPoolBackgroundQueueSize = new Integer(JsonHelper.getField(config, "threadpool.background.queuesize", "10000"));
        LOGGER.info(String.format("threadpool.background.queuesize: %d", threadPoolBackgroundQueueSize));
        Preconditions.checkArgument(threadPoolBackgroundQueueSize > 0, "threadpool.background.queuesize must be greater than 0");

        int threadPoolBackgroundSize = new Integer(JsonHelper.getField(config, "threadpool.background.size", "200"));
        LOGGER.info(String.format("threadpool.background.size: %d", threadPoolBackgroundSize));
        Preconditions.checkArgument(threadPoolBackgroundSize > 0, "threadpool.background.size must be greater than 0");

        ioQueue = new LinkedBlockingQueue<>(threadPoolIoQueueSize);
        backgroundQueue = new LinkedBlockingQueue<>(threadPoolBackgroundQueueSize);

        ioPool =
                new ThreadPoolExecutor(
                        0,
                        threadPoolIoSize,
                        60,
                        TimeUnit.SECONDS,
                        ioQueue,
                        new NamedThreadFactory("sfs-io-pool"));
        backgroundPool =
                new ThreadPoolExecutor(
                        0,
                        threadPoolBackgroundSize,
                        60,
                        TimeUnit.SECONDS,
                        backgroundQueue,
                        new NamedThreadFactory("sfs-blocking-action-pool"));

        this.vertxContext = new VertxContext<>(this);

        nodes = new Nodes(ioQueue, backgroundQueue);

        verticleMaxHeaderSize = new Integer(JsonHelper.getField(config, "http.maxheadersize", "8192"));
        LOGGER.info(String.format("http.maxheadersize: %d", verticleMaxHeaderSize));
        Preconditions.checkArgument(verticleMaxHeaderSize > 0, "http.maxheadersize must be greater than 0");

        int localNodePingInterval = new Integer(JsonHelper.getField(config, "localnode.pinginterval", String.valueOf(TimeUnit.SECONDS.toMillis(1))));
        LOGGER.info(String.format("localnode.pinginterval: %d", localNodePingInterval));
        Preconditions.checkArgument(localNodePingInterval > 0, "localnode.pinginterval must be greater than 0");

        int localNodePingTimeout = new Integer(JsonHelper.getField(config, "localnode.pingtimeout", String.valueOf(TimeUnit.SECONDS.toMillis(90))));
        LOGGER.info(String.format("localnode.pingtimeout: %d", localNodePingTimeout));
        Preconditions.checkArgument(localNodePingTimeout > 0, "localnode.pingtimeout must be greater than 0");

        remoteNodeMaxPoolSize = new Integer(JsonHelper.getField(config, "remotenode.maxpoolsize", "25"));
        LOGGER.info(String.format("remotenode.maxpoolsize: %d", remoteNodeMaxPoolSize));
        Preconditions.checkArgument(remoteNodeMaxPoolSize > 0, "remotenode.maxpoolsize must be greater than 0");

        remoteNodeConnectTimeout = new Integer(JsonHelper.getField(config, "remotenode.connectimeout", "30000"));
        LOGGER.info(String.format("remotenode.connectimeout: %d", remoteNodeConnectTimeout));
        Preconditions.checkArgument(remoteNodeConnectTimeout > 0, "remotenode.connectimeout must be greater than 0");

        int remoteNodeResponseTimeout = new Integer(JsonHelper.getField(config, "remotenode.responsetimeout", "30000"));
        LOGGER.info(String.format("remotenode.responsetimeout: %d", remoteNodeResponseTimeout));
        Preconditions.checkArgument(remoteNodeResponseTimeout > 0, "remotenode.responsetimeout must be greater than 0");

        String strRemoteNodeSecret = JsonHelper.getField(config, "remotenode.secret");
        LOGGER.info(String.format("remotenode.secret: %s", strRemoteNodeSecret));
        Preconditions.checkArgument(strRemoteNodeSecret != null, "remotenode.secret is required");
        remoteNodeSecret = BaseEncoding.base64().decode(strRemoteNodeSecret);

        int numberOfReplicas = new Integer(JsonHelper.getField(config, "number_of_replicas", "0"));
        LOGGER.info(String.format("number_of_replicas: %d", numberOfReplicas));
        Preconditions.checkArgument(numberOfReplicas >= 0, "number_of_replicas must be greater or equal to 0");

        int tempFileTtl = new Integer(JsonHelper.getField(config, "temp_file_ttl", "86400000"));
        LOGGER.info(String.format("temp_file_ttl: %d", tempFileTtl));
        Preconditions.checkArgument(tempFileTtl >= 0, "tempFileTtl must be greater or equal to 0");

        final boolean dataNode = Boolean.valueOf(JsonHelper.getField(config, "node.data", "true"));
        final boolean masterNode = Boolean.valueOf(JsonHelper.getField(config, "node.master", "true"));


        this.httpsClient = createHttpClient(vertx, true);
        this.httpClient = createHttpClient(vertx, false);

        Defer.empty()
                .flatMap(aVoid -> sfsFileSystem.open(vertxContext, fileSystem))
                .flatMap(aVoid -> fileSystemLock.lock(vertxContext))
                .flatMap(aVoid -> authProviderService.open(vertxContext))
                .flatMap(aVoid -> awsKms.start(vertxContext, config))
                .flatMap(aVoid -> azureKms.start(vertxContext, config))
                .flatMap(aVoid -> tempDirectoryCleaner.start(vertxContext, tempFileTtl))
                .flatMap(aVoid -> elasticsearch.start(vertxContext, config))
                // Don't create an http listener on this verticle
                // since the event loop gets hogged by singleton job type things
                //
                // WrapperServer creates the http listeners
                .flatMap(aVoid -> initHttpListeners(vertxContext, true))
                .flatMap(listenerList ->
                        nodes.open(vertxContext,
                                listenerList,
                                remoteNodeMaxPoolSize,
                                remoteNodeConnectTimeout,
                                remoteNodeResponseTimeout,
                                numberOfReplicas,
                                localNodePingInterval,
                                localNodePingTimeout,
                                dataNode,
                                masterNode))
                .flatMap(aVoid -> masterKeys.start(vertxContext))
                .flatMap(aVoid -> containerKeys.start(vertxContext))
                .flatMap(aVoid -> jobs.start(vertxContext, config))
                .map(aVoid -> {
                    Router router = Router.router(vertx);

                    router.get("/admin/001/healthcheck").handler(httpServerRequest -> httpServerRequest
                            .response()
                            .setStatusCode(HttpURLConnection.HTTP_OK)
                            .end());
                    // admin methods
                    router.post("/admin/001/run_jobs").handler(new SfsRequestHandler(vertxContext, new RunNodeJobs()));
                    router.post("/admin/001/dump_node").handler(new SfsRequestHandler(vertxContext, new DumpNode()));
                    router.post("/admin/001/master_keys_check").handler(new SfsRequestHandler(vertxContext, new MasterKeysCheck()));

                    // object admin method
                    router.getWithRegex("\\/admin_objectmetadata\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new GetObjectMeta()));
                    router.postWithRegex("\\/admin_objectrepair\\/[^\\/]+\\/[^\\/]+\\/.+").handler(new SfsRequestHandler(vertxContext, new RepairObject()));
                    // container admin method
                    router.postWithRegex("\\/admin_containerexport\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new ExportContainer()));
                    router.postWithRegex("\\/admin_containerimport\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new ImportContainer()));
                    // openstack keystone
                    router.post("/v2.0/tokens").handler(new SfsRequestHandler(vertxContext, new PostTokens()));
                    // cluster methods
                    router.get("/cluster/stats").handler(new SfsRequestHandler(vertxContext, new GetClusterStats()));
                    // blob store methods
                    router.delete("/blob/001").handler(new SfsRequestHandler(vertxContext, new DeleteBlob()));
                    router.get("/blob/001").handler(new SfsRequestHandler(vertxContext, new GetBlob()));
                    router.put("/blob/001").handler(new SfsRequestHandler(vertxContext, new PutBlob()));
                    router.put("/blob/001/canput").handler(new SfsRequestHandler(vertxContext, new CanPutBlob()));
                    router.put("/blob/001/ack").handler(new SfsRequestHandler(vertxContext, new AckBlob()));
                    router.get("/blob/001/checksum").handler(new SfsRequestHandler(vertxContext, new ChecksumBlob()));
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
                    router.deleteWithRegex("\\/openstackswift001\\/[^\\/]+\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new DeleteContainer()));
                    // openstack swift account methods
                    router.getWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new GetAccount()));
                    router.headWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new HeadAccount()));
                    router.postWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new PostAccount()));
                    router.deleteWithRegex("\\/openstackswift001\\/[^\\/]+").handler(new SfsRequestHandler(vertxContext, new DeleteAccount()));

                    if (testMode) {
                        router.post("/admin/001/refresh_index").handler(new SfsRequestHandler(vertxContext, new RefreshIndex()));
                        router.get("/admin/001/is_online").handler(new SfsRequestHandler(vertxContext,
                                sfsRequest ->
                                        Defer.empty()
                                                .flatMap(aVoid1 -> sfsRequest.vertxContext().verticle().nodes().isOnline(vertxContext()))
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

                    activeRouter.set(router);
                    return null;
                })
                .subscribe(
                        o -> {
                            // do nothing
                        }, throwable -> {
                            LOGGER.error("Failed to start verticle " + _this, throwable);
                            startException = throwable;
                            started = true;
                            startedResult.fail(throwable);
                        }, () -> {
                            LOGGER.info("Started verticle " + _this);
                            started = true;
                            startedResult.complete();
                        });
    }

    @Override
    public void stop(final Future<Void> stoppedResult) {
        final Server _this = this;
        LOGGER.info("Stopping verticle " + _this);

        activeRouter.set(null);

        Defer.empty()
                .flatMap(aVoid -> {
                    if (jobs != null) {
                        return jobs
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    }
                    return Defer.empty();
                })
                .flatMap(aVoid -> {
                    if (tempDirectoryCleaner != null) {
                        return tempDirectoryCleaner
                                .stop()
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    }
                    return Defer.empty();
                })
                .flatMap(aVoid -> {
                    if (containerKeys != null) {
                        return containerKeys
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    } else {
                        return Defer.empty();
                    }
                })
                .flatMap(aVoid -> {
                    if (masterKeys != null) {
                        return masterKeys
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    } else {
                        return Defer.empty();
                    }
                })
                .flatMap(aVoid -> {
                    if (nodes != null) {
                        return nodes
                                .close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    } else {
                        return Defer.empty();
                    }
                })
                .flatMap(aVoid -> {
                    if (elasticsearch != null) {
                        return elasticsearch
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    } else {
                        return Defer.empty();
                    }
                })
                .flatMap(aVoid -> {
                    if (azureKms != null) {
                        return azureKms
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    }
                    return Defer.empty();
                })
                .flatMap(aVoid -> {
                    if (awsKms != null) {
                        return awsKms
                                .stop(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    }
                    return Defer.empty();
                })
                .flatMap(aVoid -> {
                    if (authProviderService != null) {
                        return authProviderService
                                .close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return Defer.empty();
                                });
                    }
                    return Defer.empty();
                })
                .doOnNext(aVoid -> {
                    if (ioPool != null) {
                        ioPool.shutdown();
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("Unhandled Exception", throwable);
                    return Defer.empty();
                })
                .doOnNext(aVoid -> {
                    if (backgroundPool != null) {
                        backgroundPool.shutdown();
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("Unhandled Exception", throwable);
                    return Defer.empty();
                })
                .flatMap(new WaitForEmptyQueue(vertxContext, ioQueue))
                .flatMap(new WaitForEmptyQueue(vertxContext, backgroundQueue))
                .subscribe(
                        aVoid -> {
                            // do nothing
                        },
                        throwable -> {
                            STARTED.set(false);
                            LOGGER.info("Failed to stop verticle " + _this, throwable);
                            stoppedResult.fail(throwable);
                        },
                        () -> {
                            STARTED.set(false);
                            LOGGER.info("Stopped verticle " + _this);
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
    public byte[] getRemoteNodeSecret() {
        return remoteNodeSecret;
    }

    public Observable<List<TransientXListener>> initHttpListeners(VertxContext<Server> vertxContext, boolean createHttpServer) {
        // for each listen address attempt to listen on the supplied port range
        // and if successful add to the listener list which will be used
        // to initialize the rest of the application
        Set<String> listeningHostAddresses = new HashSet<>();
        return RxHelper.iterate(parsedListenAddresses, hostAndPort -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Parsed listen ports " + parsedListenAddresses);
            }
            if (!listeningHostAddresses.contains(hostAndPort.getHostText())) {
                if (createHttpServer) {
                    LOGGER.info("Creating http listener on " + hostAndPort);
                    return createHttpServer(vertxContext, hostAndPort, verticleMaxHeaderSize)
                            .onErrorResumeNext(throwable -> {
                                LOGGER.warn("Failed to start listener " + hostAndPort.toString(), throwable);
                                return Defer.just(null);
                            })
                            .map(httpServer -> {
                                if (httpServer != null) {
                                    listeningHostAddresses.add(hostAndPort.getHostText());
                                }
                                return Boolean.TRUE;
                            });
                } else {
                    LOGGER.debug("Skip creating http listener");
                    listeningHostAddresses.add(hostAndPort.getHostText());
                    return Observable.just(true);
                }
            }
            return Defer.just(true);
        })
                .map(aBoolean -> {
                    List<TransientXListener> listenerList = new ArrayList<>();
                    for (HostAndPort publishAddress : parsedPublishAddresses) {
                        listenerList.add(new TransientXListener()
                                .setHostAndPort(publishAddress));

                    }
                    return listenerList;
                });
    }

    protected Observable<HttpServer> createHttpServer(VertxContext<Server> vertxContext, HostAndPort hostAndPort, int verticleMaxHeaderSize) {
        AsyncResultMemoizeHandler<HttpServer, HttpServer> handler = new AsyncResultMemoizeHandler<>();
        HttpServerOptions httpServerOptions = new HttpServerOptions()
                .setMaxHeaderSize(verticleMaxHeaderSize)
                .setCompressionSupported(false)
                .setUsePooledBuffers(true)
                .setAcceptBacklog(10000)
                .setReuseAddress(true);
        vertxContext.vertx().createHttpServer(httpServerOptions)
                .requestHandler(httpServerRequest -> {
                    Router router = activeRouter.get();
                    if (router != null) {
                        router.accept(httpServerRequest);
                    } else {
                        httpServerRequest.response()
                                .setStatusCode(HttpURLConnection.HTTP_NOT_FOUND)
                                .end();
                    }
                })
                .listen(hostAndPort.getPort(), hostAndPort.getHostText(), handler);
        return Observable.create(handler.subscribe);
    }

    protected HttpClient createHttpClient(Vertx v, boolean https) {
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(remoteNodeConnectTimeout)
                .setMaxPoolSize(remoteNodeMaxPoolSize)
                .setKeepAlive(true)
                .setUsePooledBuffers(true)
                .setPipelining(false)
                .setMaxWaitQueueSize(200)
                .setReuseAddress(true)
                .setSsl(https);

        return v.createHttpClient(httpClientOptions);
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
            routingContext.addBodyEndHandler(event -> routingContext.request().resume());
            SfsRequest sfsRequest = new SfsRequest(vertxContext, routingContext.request());
            delegate.handle(sfsRequest);
        }
    }
}

