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

package org.sfs.integration.java;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.sfs.Server;
import org.sfs.SfsServer;
import org.sfs.TestSubscriber;
import org.sfs.VertxContext;
import org.sfs.integration.java.func.WaitForCluster;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.elasticsearch.common.settings.Settings.Builder;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.UUIDGen.getTimeUUID;
import static rx.Observable.create;
import static rx.Observable.from;
import static rx.Observable.just;

@RunWith(VertxUnitRunner.class)
public class BaseTestVerticle {

    private static final Logger LOGGER = getLogger(BaseTestVerticle.class);
    protected Path ROOT_TMP_DIR;
    private Path ES_TMP_DIR;
    private String ES_CLUSTER_NAME;
    private String ES_NODE_NAME;
    private String ES_HOST;
    protected Client ES_CLIENT;
    protected Path tmpDir;
    private Node ES_NODE;
    protected Vertx VERTX;
    protected HttpClient HTTP_CLIENT;
    protected HttpClient HTTPS_CLIENT;
    protected VertxContext<Server> VERTX_CONTEXT;

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();


    @Before
    public void before(TestContext context) {
        Async async = context.async();
        empty()
                .flatMap(aVoid -> {
                    VERTX = rule.vertx();
                    String clusteruuid = getTimeUUID().toString();
                    try {
                        ROOT_TMP_DIR = createTempDirectory("");
                        ES_TMP_DIR = createTempDirectory(ROOT_TMP_DIR, format("test-cluster-%s", clusteruuid));
                        tmpDir = createTempDirectory(ROOT_TMP_DIR, valueOf(currentTimeMillis()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    int esPort = findFreePort(9300, 9400);
                    ES_HOST = "127.0.0.1:" + esPort;
                    ES_CLUSTER_NAME = format("test-cluster-%s", clusteruuid);
                    ES_NODE_NAME = format("test-server-node-%s", clusteruuid);

                    Builder settings = settingsBuilder();
                    settings.put("script.groovy.sandbox.enabled", false);
                    settings.put("cluster.name", ES_CLUSTER_NAME);
                    settings.put("node.name", ES_NODE_NAME);
                    settings.put("http.enabled", false);
                    settings.put("discovery.zen.ping.multicast.enabled", false);
                    settings.put("discovery.zen.ping.unicast.hosts", ES_HOST);
                    settings.put("transport.tcp.port", esPort);
                    settings.put("network.host", "127.0.0.1");
                    settings.put("node.data", true);
                    settings.put("node.master", true);
                    settings.put("path.home", ES_TMP_DIR);
                    ES_NODE = nodeBuilder()
                            .settings(settings)
                            .node();
                    ES_CLIENT = ES_NODE.client();

                    JsonObject verticleConfig;

                    Buffer buffer = VERTX.fileSystem().readFileBlocking(currentThread().getContextClassLoader().getResource("intgtestconfig.json").getFile());
                    verticleConfig = new JsonObject(buffer.toString(UTF_8));
                    verticleConfig.put("fs.home", tmpDir.toString());

                    if (!verticleConfig.containsKey("elasticsearch.cluster.name")) {
                        verticleConfig.put("elasticsearch.cluster.name", ES_CLUSTER_NAME);
                    }

                    if (!verticleConfig.containsKey("elasticsearch.node.name")) {
                        verticleConfig.put("elasticsearch.node.name", ES_NODE_NAME);
                    }

                    if (!verticleConfig.containsKey("elasticsearch.discovery.zen.ping.unicast.hosts")) {
                        verticleConfig.put("elasticsearch.discovery.zen.ping.unicast.hosts", new JsonArray().add(ES_HOST));
                    }

                    if (!verticleConfig.containsKey("http.listen.port")) {
                        verticleConfig.put("http.listen.port", valueOf(findFreePort(6677, 7777)));
                    }

                    HttpClientOptions httpClientOptions = new HttpClientOptions();
                    httpClientOptions.setDefaultPort(parseInt(verticleConfig.getString("http.listen.port")))
                            .setDefaultHost("127.0.0.1")
                            .setMaxPoolSize(25)
                            .setConnectTimeout(1000)
                            .setKeepAlive(false)
                            .setLogActivity(true);

                    HttpClientOptions httpsClientOptions = new HttpClientOptions();
                    httpsClientOptions.setDefaultPort(parseInt(verticleConfig.getString("http.listen.port")))
                            .setDefaultHost("127.0.0.1")
                            .setMaxPoolSize(25)
                            .setConnectTimeout(1000)
                            .setKeepAlive(false)
                            .setLogActivity(true)
                            .setSsl(true);
                    HTTP_CLIENT = VERTX.createHttpClient(httpClientOptions);
                    HTTPS_CLIENT = VERTX.createHttpClient(httpsClientOptions);

                    SfsServer sfsServer = new SfsServer();

                    AsyncResultMemoizeHandler<String, String> handler = new AsyncResultMemoizeHandler<>();
                    VERTX.deployVerticle(
                            sfsServer,
                            new DeploymentOptions()
                                    .setConfig(verticleConfig),
                            handler);
                    return create(handler.subscribe)
                            .map(new ToVoid<>())
                            .doOnNext(aVoid1 -> {
                                VERTX_CONTEXT = sfsServer.vertxContext();
                                checkState(VERTX_CONTEXT != null, "VertxContext was null on Verticle %s", sfsServer);
                            })
                            .flatMap(new WaitForCluster(VERTX, HTTP_CLIENT))
                            .onErrorResumeNext(throwable -> {
                                throwable.printStackTrace();
                                return cleanup().flatMap(aVoid1 -> Observable.<Void>error(throwable));
                            });
                }).subscribe(new TestSubscriber(context, async));
    }

    protected VertxContext<Server> vertxContext() {
        return VERTX_CONTEXT;
    }

    protected Observable<Void> cleanup() {
        Set<String> deploymentIds = VERTX.deploymentIDs();
        return from(deploymentIds)
                .flatMap(deploymentId -> {
                    AsyncResultMemoizeHandler<Void, Void> handler = new AsyncResultMemoizeHandler<>();
                    VERTX.undeploy(deploymentId, handler);
                    return create(handler.subscribe);
                })
                .onErrorResumeNext(throwable -> {
                    throwable.printStackTrace();
                    return just(null);
                })
                .doOnNext(aVoid -> {
                    if (ES_NODE != null) {
                        ES_NODE.close();
                    }
                    if (ROOT_TMP_DIR != null) {
                        VERTX.fileSystem().deleteRecursiveBlocking(ROOT_TMP_DIR.toString(), true);
                    }
                })
                .doOnNext(aVoid -> {
                    ROOT_TMP_DIR = null;
                    ES_TMP_DIR = null;
                    ES_CLUSTER_NAME = null;
                    ES_NODE_NAME = null;
                    ES_HOST = null;
                    ES_CLIENT = null;
                    tmpDir = null;
                    ES_NODE = null;
                    VERTX = null;
                    HTTP_CLIENT = null;
                    VERTX_CONTEXT = null;
                });
    }

    @After
    public void after(TestContext context) {
        Async async = context.async();
        cleanup()
                .subscribe(new TestSubscriber(context, async));
    }

    private static int findFreePort(int first, int last) {
        while (first <= last) {
            try (ServerSocket serverSocket = new ServerSocket(first)) {
                return first;
            } catch (IOException e) {
                first++;
                // do nothing since we're search for an open port
            }
        }
        throw new RuntimeException("Unable to find free port in range [" + first + " - " + last + "]");
    }
}
