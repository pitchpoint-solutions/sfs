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

package org.sfs.util;


import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Arrays;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class ConfigHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

    private static NavigableSet<String> USED_ENV_VARS = new ConcurrentSkipListSet<>();

    public static Iterable<String> getArrayFieldOrEnv(JsonObject config, String name, String[] defaultValue) {
        return getArrayFieldOrEnv(config, name, Arrays.asList(defaultValue));
    }

    public static Iterable<String> getArrayFieldOrEnv(JsonObject config, String name, Iterable<String> defaultValue) {
        String envVar = formatEnvVariable(name);
        //USED_ENV_VARS.add(envVar);
        if (config.containsKey(name)) {
            JsonArray values = config.getJsonArray(name);
            if (values != null) {
                Iterable<String> iterable =
                        FluentIterable.from(values)
                                .filter(Predicates.notNull())
                                .filter(input -> input instanceof String)
                                .transform(input -> input.toString());
                log(name, envVar, name, iterable);
                return iterable;
            }
        } else {
            String value = System.getenv(envVar);
            if (value != null) {
                log(name, envVar, envVar, value);
                return Splitter.on(',').split(value);
            }
        }
        log(name, envVar, null, defaultValue);
        return defaultValue;
    }

    public static String getFieldOrEnv(JsonObject config, String name, String defaultValue) {
        String envVar = formatEnvVariable(name);
        //USED_ENV_VARS.add(envVar);
        if (config.containsKey(name)) {
            Object value = config.getValue(name);
            if (value != null) {
                String valueAsString = value.toString();
                log(name, envVar, name, valueAsString);
                return valueAsString;
            }
        } else {
            String value = System.getenv(envVar);
            if (value != null) {
                log(name, envVar, envVar, value);
                return value;
            }
        }
        log(name, envVar, null, defaultValue);
        return defaultValue;
    }

    public static String getFieldOrEnv(JsonObject config, String name) {
        return getFieldOrEnv(config, name, null);
    }

    public static NavigableSet<String> getEnvVars() {
        return USED_ENV_VARS;
    }

    protected static String formatEnvVariable(String name) {
        Iterable<String> splits = Splitter.on('.').split(name);
        Iterable<String> upperCased =
                FluentIterable.from(splits)
                        .transform(String::toUpperCase);
        String joined = "SFS_" + Joiner.on('_').join(upperCased);
        return joined;
    }

    protected static void log(String name, String envVar, String foundName, String value) {
        if (foundName != null) {
            LOGGER.info(String.format("Config['%s'], Env['%s']. Found value in %s: %s", name, envVar, foundName, value));
        } else {
            LOGGER.info(String.format("Config['%s'], Env['%s']. Value not found using default: %s", name, envVar, value));
        }
    }

    protected static void log(String name, String envVar, String foundName, Iterable<String> value) {
        if (LOGGER.isInfoEnabled()) {
            if (foundName != null) {
                LOGGER.info(String.format("Config['%s'], Env['%s']. Found value in %s: %s", name, envVar, foundName, Iterables.toString(value)));
            } else {
                LOGGER.info(String.format("Config['%s'], Env['%s']. Value not found using default: %s", name, envVar, value));
            }
        }
    }
}
