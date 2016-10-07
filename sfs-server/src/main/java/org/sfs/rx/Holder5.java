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

package org.sfs.rx;

public class Holder5<A, B, C, D, E> {

    public A value0;
    public B value1;
    public C value2;
    public D value3;
    public E value4;

    public Holder5() {
    }

    public Holder5(A value0, B value1, C value2, D value3, E value4) {
        this.value0 = value0;
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
        this.value4 = value4;
    }

    public Holder5<A, B, C, D, E> value0(A value) {
        this.value0 = value;
        return this;
    }

    public Holder5<A, B, C, D, E> value1(B value) {
        this.value1 = value;
        return this;
    }

    public Holder5<A, B, C, D, E> value2(C value) {
        this.value2 = value;
        return this;
    }

    public Holder5<A, B, C, D, E> value3(D value) {
        this.value3 = value;
        return this;
    }


    public Holder5<A, B, C, D, E> value4(E value) {
        this.value4 = value;
        return this;
    }

    public A value0() {
        return value0;
    }

    public B value1() {
        return value1;
    }

    public C value2() {
        return value2;
    }

    public D value3() {
        return value3;
    }

    public E value4() {
        return value4;
    }

    @Override
    public String toString() {
        return "Holder5{" +
                "value0=" + value0 +
                ", value1=" + value1 +
                ", value2=" + value2 +
                ", value3=" + value3 +
                ", value4=" + value4 +
                '}';
    }
}
