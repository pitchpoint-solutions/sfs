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

package org.sfs.rx;

public class Holder2<A, B> {

    public A value0;
    public B value1;

    public Holder2() {
    }

    public Holder2(A value0, B value1) {
        this.value0 = value0;
        this.value1 = value1;
    }

    public Holder2<A, B> value0(A value) {
        this.value0 = value;
        return this;
    }

    public Holder2<A, B> value1(B value) {
        this.value1 = value;
        return this;
    }

    public A value0() {
        return value0;
    }

    public B value1() {
        return value1;
    }

    @Override
    public String toString() {
        return "Holder2{" +
                "value0=" + value0 +
                ", value1=" + value1 +
                '}';
    }
}
