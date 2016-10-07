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

package org.sfs.validate;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.vo.ObjectPath;
import rx.functions.Func1;

import static java.lang.Character.isLetterOrDigit;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static org.sfs.util.ByteLength.utf8length;

public abstract class ValidatePath implements Func1<ObjectPath, ObjectPath> {

    private final boolean includeAccount;
    private final boolean includeContainer;
    private final boolean includeObject;

    public ValidatePath(boolean includeAccount, boolean includeContainer, boolean includeObject) {
        this.includeAccount = includeAccount;
        this.includeContainer = includeContainer;
        this.includeObject = includeObject;
    }

    @Override
    public ObjectPath call(ObjectPath objectPath) {
        if (includeAccount) {
            Optional<String> oAccount = objectPath.accountName();
            if (!oAccount.isPresent()) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Account name is required");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);

            }
            String account = oAccount.get();
            if (utf8length(account) > 256) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Max account name length is 256 bytes");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }
        }
        if (includeContainer) {
            Optional<String> oContainer = objectPath.containerName();
            if (!oContainer.isPresent()) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Container name is required");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);

            }
            String container = oContainer.get();
            if (utf8length(container) > 256) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Max container name length is 256 bytes");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }

            for (char c : container.toCharArray()) {
                if (!isLetterOrDigit(c) && c != '-') {
                    JsonObject jsonObject = new JsonObject()
                            .put("message", "Container name contain only letters, digit and dashes");
                    throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
                }
            }
        }
        if (includeObject) {
            Optional<String> oObject = objectPath.objectName();
            if (!oObject.isPresent()) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Object name is required");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);

            }
            String object = oObject.get();
            if (utf8length(object) > 2048) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", "Max object name length is 2048 bytes");
                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }
        }

        return objectPath;
    }
}
