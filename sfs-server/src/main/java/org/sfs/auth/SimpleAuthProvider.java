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

package org.sfs.auth;

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.google.common.escape.Escaper;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.LimitedWriteEndableWriteStream;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.TransientAccount;
import org.sfs.vo.TransientContainer;
import org.sfs.vo.TransientVersion;
import rx.Observable;

import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Objects.equal;
import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Splitter.on;
import static com.google.common.collect.HashMultimap.create;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Calendar.getInstance;
import static org.sfs.auth.Role.ADMIN;
import static org.sfs.auth.Role.USER;
import static org.sfs.auth.Role.fromValueIfExists;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.JsonHelper.getField;
import static org.sfs.util.Limits.MAX_AUTH_REQUEST_SIZE;
import static org.sfs.util.SfsHttpHeaders.X_AUTH_TOKEN;
import static org.sfs.util.SfsHttpUtil.getRemoteServiceUrl;
import static rx.Observable.just;

public class SimpleAuthProvider implements AuthProvider {

    private SetMultimap<Role, User> roles = create();

    @Override
    public Observable<Void> open(VertxContext<Server> vertxContext) {
        JsonObject config = vertxContext.verticle().config();
        JsonObject jsonObject = config.getJsonObject("auth");
        if (jsonObject != null) {
            for (String roleName : jsonObject.fieldNames()) {
                Role role = fromValueIfExists(roleName);
                checkState(role != null, "%s is not a valid role", roleName);
                JsonArray jsonUsers = jsonObject.getJsonArray(roleName);
                if (jsonUsers != null) {
                    for (Object o : jsonUsers) {
                        JsonObject jsonUser = (JsonObject) o;
                        String id = getField(jsonUser, "id");
                        String username = getField(jsonUser, "username");
                        String password = getField(jsonUser, "password");
                        roles.put(role, new User(id, username, password));
                    }
                }
            }
        } else {
            roles.clear();
        }
        return aVoid();
    }

    @Override
    public int priority() {
        return 0;
    }

    @Override
    public Observable<Void> close(VertxContext<Server> vertxContext) {
        roles.clear();
        return aVoid();
    }

    @Override
    public Observable<Void> authenticate(SfsRequest sfsRequest) {
        return aVoid()
                .doOnNext(aVoid -> {
                    UserAndRole userAndRole = getUserByCredentials(sfsRequest);
                    sfsRequest.setUserAndRole(userAndRole);
                });
    }

    @Override
    public Observable<Boolean> isAuthenticated(SfsRequest sfsRequest) {
        return just(sfsRequest.getUserAndRole() != null);
    }

    @Override
    public void handleOpenstackKeystoneAuth(SfsRequest httpServerRequest) {
        httpServerRequest.pause();
        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        aVoid()
                .flatMap(aVoid -> {
                    final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                    LimitedWriteEndableWriteStream limitedWriteStream = new LimitedWriteEndableWriteStream(bufferWriteStream, MAX_AUTH_REQUEST_SIZE);
                    return pump(httpServerRequest, limitedWriteStream)
                            .map(aVoid1 -> bufferWriteStream.toBuffer());
                })
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(jsonObject -> {
                    JsonObject authJsonObject = jsonObject.getJsonObject("auth");
                    JsonObject passwordCredentialsJson = authJsonObject.getJsonObject("passwordCredentials");
                    String username = passwordCredentialsJson.getString("username");
                    String password = passwordCredentialsJson.getString("password");
                    String tenantName = authJsonObject.getString("tenantName");
                    if (tenantName == null) {
                        tenantName = "default";
                    }

                    Set<String> selectedRoles = new HashSet<>();

                    for (Map.Entry<Role, Collection<User>> entry : roles.asMap().entrySet()) {
                        Role role = entry.getKey();
                        Collection<User> users = entry.getValue();
                        for (User user : users) {
                            if (equal(user.getUsername(), username)
                                    && equal(user.getPassword(), password)) {
                                selectedRoles.add(role.value());
                            }
                        }
                    }

                    if (selectedRoles.isEmpty()) {
                        JsonObject errorJson = new JsonObject()
                                .put("message", "Invalid Credentials");
                        throw new HttpRequestValidationException(HTTP_FORBIDDEN, errorJson);
                    }

                    Escaper escaper = urlPathSegmentEscaper();

                    String serviceUrl = getRemoteServiceUrl(httpServerRequest);
                    serviceUrl = format("%s/openstackswift001/%s", serviceUrl, escaper.escape(tenantName));

                    JsonObject endpointJsonObject =
                            new JsonObject()
                                    .put("region", "ORD")
                                    .put("tenantId", tenantName)
                                    .put("publicURL", serviceUrl)
                                    .put("internalURL", serviceUrl);

                    JsonArray endpointsJsonArray =
                            new JsonArray()
                                    .add(endpointJsonObject);

                    JsonObject serviceCatalogJsonObject =
                            new JsonObject()
                                    .put("type", "object-store")
                                    .put("name", "openstackswift001")
                                    .put("endpoints", endpointsJsonArray);

                    JsonArray serviceCatalogJsonArray =
                            new JsonArray()
                                    .add(serviceCatalogJsonObject);

                    JsonObject userJsonObject =
                            new JsonObject()
                                    .put("username", username)
                                    .put("roles_links", new JsonArray())
                                    .put("id", username);

                    JsonArray roles = new JsonArray();
                    for (String selectedRole : selectedRoles) {
                        roles.add(selectedRole);
                    }

                    userJsonObject = userJsonObject.put("roles", roles);

                    Calendar expiresDt = getInstance();
                    expiresDt.setTimeInMillis(currentTimeMillis() + 86400000);

                    JsonObject tokenJsonObject =
                            new JsonObject()
                                    .put("audit_ids", new JsonArray())
                                    .put("expires", toDateTimeString(expiresDt))
                                    .put("issued_at", toDateTimeString(getInstance()))
                                    .put("id", base64().encode((username + ":" + password).getBytes(UTF_8)));

                    JsonObject metadataJsonObject =
                            new JsonObject()
                                    .put("is_admin", 0)
                                    .put("roles", new JsonArray());

                    return new JsonObject()
                            .put("access",
                                    new JsonObject()
                                            .put("serviceCatalog", serviceCatalogJsonArray)
                                            .put("token", tokenJsonObject)
                                            .put("user", userJsonObject)
                                            .put("metadata", metadataJsonObject)
                            );
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<JsonObject>(httpServerRequest) {
                    @Override
                    public void onNext(JsonObject authenticationResponse) {
                        Buffer buffer = buffer(authenticationResponse.encode(), UTF_8.toString());
                        httpServerRequest.response().setStatusCode(HTTP_OK)
                                .putHeader(CONTENT_LENGTH, valueOf(buffer.length()))
                                .write(buffer);
                    }
                });
    }

    @Override
    public Observable<Boolean> canObjectUpdate(SfsRequest sfsRequest, TransientVersion version) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canObjectDelete(SfsRequest sfsRequest, TransientVersion version) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canObjectCreate(SfsRequest sfsRequest, TransientVersion version) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canObjectRead(SfsRequest sfsRequest, TransientVersion version) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canContainerUpdate(SfsRequest sfsRequest, PersistentContainer container) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canContainerDelete(SfsRequest sfsRequest, PersistentContainer container) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canContainerCreate(SfsRequest sfsRequest, TransientContainer container) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canContainerRead(SfsRequest sfsRequest, PersistentContainer container) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    @Override
    public Observable<Boolean> canAccountUpdate(SfsRequest sfsRequest, PersistentAccount account) {
        return aVoid()
                .map(aVoid -> isAdmin(sfsRequest));
    }

    @Override
    public Observable<Boolean> canAccountDelete(SfsRequest sfsRequest, PersistentAccount account) {
        return aVoid()
                .map(aVoid -> isAdmin(sfsRequest));
    }

    @Override
    public Observable<Boolean> canAccountCreate(SfsRequest sfsRequest, TransientAccount account) {
        return aVoid()
                .map(aVoid -> isAdmin(sfsRequest));
    }

    @Override
    public Observable<Boolean> canAccountRead(SfsRequest sfsRequest, PersistentAccount account) {
        return aVoid()
                .map(aVoid -> isAdmin(sfsRequest));
    }

    @Override
    public Observable<Boolean> canAdmin(SfsRequest sfsRequest) {
        return aVoid()
                .map(aVoid -> isAdmin(sfsRequest));
    }

    @Override
    public Observable<Boolean> canContainerListObjects(SfsRequest sfsRequest, PersistentContainer container) {
        return aVoid()
                .map(aVoid -> isAdminOrUser(sfsRequest));
    }

    protected boolean isAdminOrUser(SfsRequest sfsRequest) {
        UserAndRole userAndRole = sfsRequest.getUserAndRole();
        return userAndRole != null && (ADMIN.equals(userAndRole.getRole()) || USER.equals(userAndRole.getRole()));
    }

    protected boolean isAdmin(SfsRequest sfsRequest) {
        UserAndRole userAndRole = sfsRequest.getUserAndRole();
        return userAndRole != null && ADMIN.equals(userAndRole.getRole());
    }

    protected UserAndRole getUserByCredentials(SfsRequest sfsRequest) {

        MultiMap headers = sfsRequest.headers();
        Optional<String> oToken;
        if (headers.contains(X_AUTH_TOKEN)) {
            oToken = fromNullable(headers.get(X_AUTH_TOKEN));
        } else if (headers.contains(AUTHORIZATION)) {
            oToken = extractToken(headers.get(AUTHORIZATION), "Basic");
        } else {
            oToken = absent();
        }

        if (oToken.isPresent()) {
            String token = oToken.get();

            String decoded = new String(base64().decode(token), StandardCharsets.UTF_8);
            String[] parts =
                    toArray(
                            on(':')
                                    .limit(2)
                                    .split(decoded),
                            String.class
                    );

            if (parts.length == 2) {
                String username = parts[0];
                String password = parts[1];
                for (Role role : new Role[]{ADMIN, USER}) {
                    Set<User> usersForRole = this.roles.get(role);
                    if (usersForRole != null) {
                        for (User user : usersForRole) {
                            if (equal(user.getUsername(), username)
                                    && equal(user.getPassword(), password)) {
                                return new UserAndRole(role, user);
                            }
                        }
                    }
                }
            }
        }
        return null;
    }


    protected Optional<String> extractToken(String authorizationHeaderValue, String authorizationType) {
        if (authorizationHeaderValue != null) {
            authorizationHeaderValue = WHITESPACE.trimLeadingFrom(authorizationHeaderValue);
            if (authorizationHeaderValue.regionMatches(true, 0, authorizationType, 0, authorizationType.length())) {
                String[] values =
                        toArray(
                                on(' ')
                                        .limit(2)
                                        .split(authorizationHeaderValue),
                                String.class);

                return of(values[1]);
            }
        }
        return absent();
    }

}
