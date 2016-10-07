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

package org.sfs.encryption;

import org.sfs.Server;
import org.sfs.VertxContext;

public class KmsFactory {

    public static final String AZURE_PREFIX = "xppsazure:";
    public static final String AWS_PREFIX = "xppsaws:";

    public static Kms newKms(VertxContext<Server> vertxContext) {
        return vertxContext.verticle().awsKms();
    }

    public static Kms newBackup0Kms(VertxContext<Server> vertxContext) {
        return vertxContext.verticle().azureKms();
    }

    public static Kms fromKeyId(VertxContext<Server> vertxContext, String keyId) {
        if (keyId.startsWith(AWS_PREFIX)
                // support legacy prefix
                || keyId.startsWith("arn:aws:kms:")) {
            return vertxContext.verticle().awsKms();
        } else if (keyId.startsWith(AZURE_PREFIX)) {
            return vertxContext.verticle().azureKms();
        } else {
            throw new IllegalStateException(keyId + " is not prefixed with a supported key type");
        }
    }
}
