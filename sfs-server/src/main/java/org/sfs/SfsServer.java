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

package org.sfs;

import com.fasterxml.jackson.core.JsonFactory;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.sfs.auth.AuthProviderService;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.encryption.AwsKms;
import org.sfs.encryption.AzureKms;
import org.sfs.encryption.ContainerKeys;
import org.sfs.encryption.MasterKeys;
import org.sfs.filesystem.temp.TempDirectoryCleaner;
import org.sfs.jobs.Jobs;
import org.sfs.nodes.Nodes;
import org.sfs.vo.TransientXListener;
import rx.Subscriber;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class SfsServer extends Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(SfsServer.class);
    private static final String KEY = "sfs.server.delegate";
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
        SfsSingletonServer candidate = new SfsSingletonServer();
        if (setDelegate(candidate)) {
            candidate.init(vertx, vertx.getOrCreateContext());
            candidate.start(Future.future());
        }
        vertx.setPeriodic(100, event -> {
            SfsSingletonServer server = getDelegate();
            if (server != null && server.isStarted()) {
                Throwable startException = server.getStartException();
                vertx.cancelTimer(event);
                if (startException == null) {
                    vertxContext = new VertxContext<Server>(this);
                    httpsClient = server.createHttpClient(vertx, true);
                    httpClient = server.createHttpClient(vertx, false);
                    server.initHttpListeners(vertxContext, true)
                            .subscribe(new Subscriber<List<TransientXListener>>() {
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
                                public void onNext(List<TransientXListener> listenerList) {

                                }
                            });
                } else {
                    LOGGER.error("Failed to start verticle " + _this, startException);
                    startedResult.fail(startException);
                }
            }
        });
    }

    @Override
    public void stop(Future<Void> stoppedResult) {
        SfsServer _this = this;
        LOGGER.info("Stopping verticle " + _this);
        SfsSingletonServer sfsSingletonServer = (SfsSingletonServer) vertx.sharedData().getLocalMap(KEY).remove("a");
        if (sfsSingletonServer != null) {
            sfsSingletonServer.stop(stoppedResult);
        } else {
            LOGGER.info("Stopped verticle " + _this);
            stoppedResult.complete();
        }
    }

    protected boolean setDelegate(SfsSingletonServer sfsSingletonServer) {
        return vertx.sharedData().getLocalMap(KEY).putIfAbsent("a", sfsSingletonServer) == null;
    }

    protected SfsSingletonServer getDelegate() {
        if (delegate == null) {
            delegate = (SfsSingletonServer) vertx.sharedData().getLocalMap(KEY).get("a");
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
    public byte[] getRemoteNodeSecret() {
        return getDelegate().getRemoteNodeSecret();
    }

    @Override
    public VertxContext<Server> vertxContext() {
        return vertxContext;
    }

}
