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

public class Holder1<A> {

    public A value;

    public Holder1() {
    }

    public Holder1(A value) {
        this.value = value;
    }

    public Holder1<A> value(A value) {
        this.value = value;
        return this;
    }

    public A value() {
        return value;
    }

    @Override
    public String toString() {
        return "Holder1{" +
                "value=" + value +
                '}';
    }
}
