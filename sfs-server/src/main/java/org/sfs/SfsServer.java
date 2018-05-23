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
import com.google.common.net.HostAndPort;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
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
import org.sfs.rx.Defer;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;
import rx.plugins.RxJavaHooks;
import rx.plugins.RxJavaSchedulersHook;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class SfsServer extends Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(SfsServer.class);
    private static final AtomicBoolean first = new AtomicBoolean(true);
    public static final String KEY = "sfs.server.delegate";
    public static final String ID = "a";
    private VertxContext<Server> vertxContext;
    private SfsSingletonServer delegate;
    private HttpClient httpClient;
    private HttpClient httpsClient;

    public SfsServer() {
    }

    @Override
    public void start(Future<Void> startedResult) {
        SfsServer _this = this;
        LOGGER.info("Starting verticle " + _this);
        initRxSchedulers(context);

        ObservableFuture<Lock> lockObservableFuture = RxHelper.observableFuture();
        vertx.setPeriodic(100, event -> {
            SfsSingletonServer sfsSingletonServer = getDelegate();
            if (sfsSingletonServer != null) {
                vertx.cancelTimer(event);
                Throwable e = sfsSingletonServer.getStartException();
                if (e != null) {
                    lockObservableFuture.toHandler().handle(Future.failedFuture(e));
                } else {
                    lockObservableFuture.toHandler().handle(Future.succeededFuture());
                }
            }
        });
        if (first.compareAndSet(true, false)) {
            vertx.deployVerticle(SfsSingletonServer.class.getName(),
                    new DeploymentOptions()
                            .setConfig(config())
                            .setInstances(1));
        }


        lockObservableFuture
                .flatMap(aVoid -> {
                    SfsSingletonServer delegate = getDelegate();
                    httpsClient = delegate.createHttpClient(vertx, true);
                    httpClient = delegate.createHttpClient(vertx, false);
                    vertxContext = new VertxContext<>(_this);
                    return delegate.initHttpListeners(vertxContext).map(new ToVoid<>());
                })
                .flatMap(aVoid -> {
                    // make httpclient bind to correct context
                    HostAndPort firstPublishedAddress = delegate.nodes().getHostAndPort();
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
                .count()
                .map(new ToVoid<>())
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        LOGGER.info("Started verticle " + _this);
                        startedResult.complete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        LOGGER.error("Failed to start verticle " + _this, e);
                        startedResult.fail(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }

    @Override
    public void stop(Future<Void> stoppedResult) {
        SfsServer _this = this;
        LOGGER.info("Stopping verticle " + _this);
        first.compareAndSet(false, true);
        Defer.aVoid()
                .count()
                .map(new ToVoid<>())
                .subscribe(
                        aVoid -> {
                            // do nothing
                        },
                        throwable -> {
                            LOGGER.info("Failed to stop verticle " + _this, throwable);
                            stoppedResult.fail(throwable);
                        },
                        () -> {
                            LOGGER.info("Stopped verticle " + _this);
                            stoppedResult.complete();
                        });


    }

    @Override
    public Context getContext() {
        return context;
    }

    public static void initRxSchedulers(Context context) {
        RxJavaSchedulersHook hook = RxHelper.schedulerHook(context);
        RxJavaHooks.setOnIOScheduler(f -> hook.getIOScheduler());
        RxJavaHooks.setOnNewThreadScheduler(f -> hook.getNewThreadScheduler());
        RxJavaHooks.setOnComputationScheduler(f -> hook.getComputationScheduler());
    }

    public static void setDelegate(Vertx vertx, SfsSingletonServer sfsSingletonServer) {
        Preconditions.checkState(vertx.sharedData().getLocalMap(KEY).putIfAbsent(ID, sfsSingletonServer) == null, "Concurrent single config initialization");
    }

    public static void removeDelegate(Vertx vertx, SfsSingletonServer sfsSingletonServer) {
        vertx.sharedData().getLocalMap(KEY).removeIfPresent(ID, sfsSingletonServer);
    }

    public static void testCleanup(Vertx vertx) {
        vertx.sharedData().getLocalMap(KEY).remove(ID);
    }

    protected SfsSingletonServer getDelegate() {
        if (delegate == null) {
            delegate = (SfsSingletonServer) vertx.sharedData().getLocalMap(KEY).get(ID);
        }
        return delegate;
    }

    @Override
    public HttpClient httpClient(boolean https) {
        return https ? httpsClient : httpClient;
    }

    @Override
    public ExecutorService getBackgroundPool() {
        return getDelegate().getBackgroundPool();
    }

    @Override
    public ExecutorService getIoPool() {
        return getDelegate().getIoPool();
    }

    @Override
    public Elasticsearch elasticsearch() {
        return getDelegate().elasticsearch();
    }

    @Override
    public JsonFactory jsonFactory() {
        return getDelegate().jsonFactory();
    }

    @Override
    public Nodes nodes() {
        return getDelegate().nodes();
    }

    @Override
    public SfsFileSystem sfsFileSystem() {
        return getDelegate().sfsFileSystem();
    }

    @Override
    public TempDirectoryCleaner tempFileFactory() {
        return getDelegate().tempFileFactory();
    }

    @Override
    public Jobs jobs() {
        return getDelegate().jobs();
    }

    @Override
    public AuthProviderService authProviderService() {
        return getDelegate().authProviderService();
    }

    @Override
    public AwsKms awsKms() {
        return getDelegate().awsKms();
    }

    @Override
    public AzureKms azureKms() {
        return getDelegate().azureKms();
    }

    @Override
    public MasterKeys masterKeys() {
        return getDelegate().masterKeys();
    }

    @Override
    public ContainerKeys containerKeys() {
        return getDelegate().containerKeys();
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return getDelegate().getClusterInfo();
    }

    @Override
    public NodeStats getNodeStats() {
        return getDelegate().getNodeStats();
    }

    @Override
    public byte[] getRemoteNodeSecret() {
        return getDelegate().getRemoteNodeSecret();
    }

    @Override
    public VertxContext<Server> vertxContext() {
        return vertxContext;
    }

}
