/*
 * Copyright 2019 The Simple File Server Authors
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

package org.sfs.encryption.impl;

import org.junit.Assert;
import org.junit.Test;
import org.sfs.encryption.Algorithm;
import org.sfs.encryption.AlgorithmDef;

import java.nio.charset.StandardCharsets;


public class SAES256v02Test {

    @Test
    public void test() {
        byte[] input = "HELLO".getBytes(StandardCharsets.UTF_8);

        AlgorithmDef algorithmDef = AlgorithmDef.SALTED_AES256_V02;
        byte[] key = algorithmDef.generateKeyBlocking();
        byte[] salt = algorithmDef.generateSaltBlocking();
        Algorithm algo = algorithmDef.create(key, salt);

        byte[] encrypted = algo.encrypt(input);
        byte[] decrypted = algo.decrypt(encrypted);
        Assert.assertArrayEquals(input, decrypted);
    }

}