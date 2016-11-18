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

package org.sfs.integration.java;

import com.google.common.net.HostAndPort;
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
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.elasticsearch.common.settings.Settings.Builder;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.sfs.rx.Defer.aVoid;
import static rx.Observable.from;
import static rx.Observable.just;

@RunWith(VertxUnitRunner.class)
public class BaseTestVerticle {

    private static final Logger LOGGER = getLogger(BaseTestVerticle.class);
    protected Path rootTmpDir;
    private Path esTempDir;
    private String esClusterName;
    private String esNodeName;
    private String esHost;
    protected Client esClient;
    protected Path tmpDir;
    private Node esNode;
    protected Vertx vertx;
    protected HttpClient httpClient;
    protected HttpClient httpsClient;
    protected VertxContext<Server> vertxContext;

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();


    @Before
    public void before(TestContext context) {
        vertx = rule.vertx();
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    String clusteruuid = UUID.randomUUID().toString();
                    try {
                        rootTmpDir = createTempDirectory("");
                        esTempDir = createTempDirectory(rootTmpDir, format("test-cluster-%s", clusteruuid));
                        tmpDir = createTempDirectory(rootTmpDir, valueOf(currentTimeMillis()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    int esPort = findFreePort(9300, 9400);
                    esHost = "127.0.0.1:" + esPort;
                    esClusterName = format("test-cluster-%s", clusteruuid);
                    esNodeName = format("test-server-node-%s", clusteruuid);

                    Builder settings = settingsBuilder();
                    settings.put("script.groovy.sandbox.enabled", false);
                    settings.put("cluster.name", esClusterName);
                    settings.put("node.name", esNodeName);
                    settings.put("http.enabled", false);
                    settings.put("discovery.zen.ping.multicast.enabled", false);
                    settings.put("discovery.zen.ping.unicast.hosts", esHost);
                    settings.put("transport.tcp.port", esPort);
                    settings.put("network.host", "127.0.0.1");
                    settings.put("node.data", true);
                    settings.put("node.master", true);
                    settings.put("path.home", esTempDir);
                    esNode = nodeBuilder()
                            .settings(settings)
                            .node();
                    esClient = esNode.client();

                    JsonObject verticleConfig;

                    Buffer buffer = vertx.fileSystem().readFileBlocking(currentThread().getContextClassLoader().getResource("intgtestconfig.json").getFile());
                    verticleConfig = new JsonObject(buffer.toString(UTF_8));
                    verticleConfig.put("fs.home", tmpDir.toString());

                    if (!verticleConfig.containsKey("elasticsearch.cluster.name")) {
                        verticleConfig.put("elasticsearch.cluster.name", esClusterName);
                    }

                    if (!verticleConfig.containsKey("elasticsearch.node.name")) {
                        verticleConfig.put("elasticsearch.node.name", esNodeName);
                    }

                    if (!verticleConfig.containsKey("elasticsearch.discovery.zen.ping.unicast.hosts")) {
                        verticleConfig.put("elasticsearch.discovery.zen.ping.unicast.hosts", new JsonArray().add(esHost));
                    }

                    if (!verticleConfig.containsKey("http.listen.addresses")) {
                        int freePort = findFreePort(6677, 7777);
                        verticleConfig.put("http.listen.addresses", new JsonArray().add(HostAndPort.fromParts("127.0.0.1", freePort).toString()));
                    }

                    HostAndPort hostAndPort = HostAndPort.fromString(verticleConfig.getJsonArray("http.listen.addresses").getString(0));

                    HttpClientOptions httpClientOptions = new HttpClientOptions();
                    httpClientOptions.setDefaultPort(hostAndPort.getPort())
                            .setDefaultHost(hostAndPort.getHostText())
                            .setMaxPoolSize(25)
                            .setConnectTimeout(1000)
                            .setKeepAlive(false)
                            .setLogActivity(true);

                    HttpClientOptions httpsClientOptions = new HttpClientOptions();
                    httpsClientOptions.setDefaultPort(hostAndPort.getPort())
                            .setDefaultHost(hostAndPort.getHostText())
                            .setMaxPoolSize(25)
                            .setConnectTimeout(1000)
                            .setKeepAlive(false)
                            .setLogActivity(true)
                            .setSsl(true);
                    httpClient = vertx.createHttpClient(httpClientOptions);
                    httpsClient = vertx.createHttpClient(httpsClientOptions);

                    SfsServer sfsServer = new SfsServer();

                    ObservableFuture<String> handler = RxHelper.observableFuture();
                    vertx.deployVerticle(
                            sfsServer,
                            new DeploymentOptions()
                                    .setConfig(verticleConfig),
                            handler.toHandler());
                    return handler
                            .map(new ToVoid<>())
                            .doOnNext(aVoid1 -> {
                                vertxContext = sfsServer.vertxContext();
                                checkState(vertxContext != null, "VertxContext was null on Verticle %s", sfsServer);
                            })
                            .onErrorResumeNext(throwable -> {
                                throwable.printStackTrace();
                                return cleanup().flatMap(aVoid1 -> Observable.<Void>error(throwable));
                            });
                }).subscribe(new TestSubscriber(context, async));
    }

    protected VertxContext<Server> vertxContext() {
        return vertxContext;
    }

    protected Observable<Void> cleanup() {
        Set<String> deploymentIds = vertx != null ? vertx.deploymentIDs() : Collections.emptySet();
        return from(deploymentIds)
                .flatMap(deploymentId -> {
                    ObservableFuture<Void> handler = RxHelper.observableFuture();
                    vertx.undeploy(deploymentId, handler.toHandler());
                    return handler;
                })
                .count()
                .map(new ToVoid<>())
                .onErrorResumeNext(throwable -> {
                    throwable.printStackTrace();
                    return just(null);
                })
                .doOnNext(aVoid -> {
                    if (esNode != null) {
                        esNode.close();
                    }
                    if (rootTmpDir != null && vertx != null) {
                        vertx.fileSystem().deleteRecursiveBlocking(rootTmpDir.toString(), true);
                    }
                })
                .doOnNext(aVoid -> {
                    rootTmpDir = null;
                    esTempDir = null;
                    esClusterName = null;
                    esNodeName = null;
                    esHost = null;
                    esClient = null;
                    tmpDir = null;
                    esNode = null;
                    vertx = null;
                    httpClient = null;
                    vertxContext = null;
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
