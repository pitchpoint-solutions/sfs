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

package org.sfs.vo;

import org.junit.Assert;
import org.junit.Test;

public class ObjectPathTest {

    @Test
    public void test() {
        String path = "/account/container/object/name/1/2/3";

        ObjectPath objectPath = ObjectPath.fromPaths(path);

        Assert.assertEquals("account", objectPath.accountName().get());
        Assert.assertEquals("container", objectPath.containerName().get());
        Assert.assertEquals("object/name/1/2/3", objectPath.objectName().get());

        Assert.assertEquals("/account", objectPath.accountPath().get());
        Assert.assertEquals("/account/container", objectPath.containerPath().get());
        Assert.assertEquals("/account/container/object/name/1/2/3", objectPath.objectPath().get());

    }

    @Test
    public void testWithTrailingSlash() {
        String path = "/account/container/object/name/1/2/3/";

        ObjectPath objectPath = ObjectPath.fromPaths(path);

        Assert.assertEquals("account", objectPath.accountName().get());
        Assert.assertEquals("container", objectPath.containerName().get());
        Assert.assertEquals("object/name/1/2/3/", objectPath.objectName().get());

        Assert.assertEquals("/account", objectPath.accountPath().get());
        Assert.assertEquals("/account/container", objectPath.containerPath().get());
        Assert.assertEquals("/account/container/object/name/1/2/3/", objectPath.objectPath().get());

    }

    @Test
    public void testWithOddCharacters0() {
        String path = "/account/container/object/name/1/////3";

        ObjectPath objectPath = ObjectPath.fromPaths(path);

        Assert.assertEquals("account", objectPath.accountName().get());
        Assert.assertEquals("container", objectPath.containerName().get());
        Assert.assertEquals("object/name/1/////3", objectPath.objectName().get());

        Assert.assertEquals("/account", objectPath.accountPath().get());
        Assert.assertEquals("/account/container", objectPath.containerPath().get());
        Assert.assertEquals("/account/container/object/name/1/////3", objectPath.objectPath().get());
    }

    @Test
    public void testWithOddCharacters1() {
        String path = "/account/container/object/name/1////../3";

        ObjectPath objectPath = ObjectPath.fromPaths(path);

        Assert.assertEquals("account", objectPath.accountName().get());
        Assert.assertEquals("container", objectPath.containerName().get());
        Assert.assertEquals("object/name/1////../3", objectPath.objectName().get());

        Assert.assertEquals("/account", objectPath.accountPath().get());
        Assert.assertEquals("/account/container", objectPath.containerPath().get());
        Assert.assertEquals("/account/container/object/name/1////../3", objectPath.objectPath().get());
    }

}