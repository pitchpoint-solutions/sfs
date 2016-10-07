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

import io.vertx.ext.unit.TestContext;
import org.junit.Assert;

public class VertxAssert {

    public static void assertNotNull(TestContext context, Object object) {
        try {
            Assert.assertNotNull(object);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotNull(TestContext context, String message, Object object) {
        try {
            Assert.assertNotNull(message, object);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNull(TestContext context, Object object) {
        try {
            Assert.assertNull(object);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNull(TestContext context, String message, Object object) {
        try {
            Assert.assertNull(message, object);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertNotSame(TestContext context, Object expected, Object actual) {
        try {
            Assert.assertNotSame(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotSame(TestContext context, String message, Object expected, Object actual) {
        try {
            Assert.assertNotSame(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertSame(TestContext context, Object expected, Object actual) {
        try {
            Assert.assertSame(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertSame(TestContext context, String message, Object expected, Object actual) {
        try {
            Assert.assertSame(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, float expected, float actual, float delta) {
        try {
            Assert.assertNotEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, float expected, float actual, float delta) {
        try {
            Assert.assertNotEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, double expected, double actual, double delta) {
        try {
            Assert.assertNotEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, double expected, double actual, double delta) {
        try {
            Assert.assertNotEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertNotEquals(TestContext context, short expected, short actual) {
        try {
            Assert.assertNotEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, short expected, short actual) {
        try {
            Assert.assertNotEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertNotEquals(TestContext context, int expected, int actual) {
        try {
            Assert.assertNotEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, int expected, int actual) {
        try {
            Assert.assertNotEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertNotEquals(TestContext context, long expected, long actual) {
        try {
            Assert.assertNotEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, long expected, long actual) {
        try {
            Assert.assertNotEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, Object expected, Object actual) {
        try {
            Assert.assertNotEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertNotEquals(TestContext context, String message, Object expected, Object actual) {
        try {
            Assert.assertNotEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertEquals(TestContext context, float expected, float actual, float delta) {
        try {
            Assert.assertEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, float expected, float actual, float delta) {
        try {
            Assert.assertEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, double expected, double actual, double delta) {
        try {
            Assert.assertEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, double expected, double actual, double delta) {
        try {
            Assert.assertEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertEquals(TestContext context, short expected, short actual) {
        try {
            Assert.assertEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, short expected, short actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, int expected, int actual) {
        try {
            Assert.assertEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, int expected, int actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertEquals(TestContext context, long expected, long actual) {
        try {
            Assert.assertEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, long expected, long actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, Object expected, Object actual) {
        try {
            Assert.assertEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertEquals(TestContext context, String message, Object expected, Object actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertFalse(TestContext context, boolean condition) {
        try {
            Assert.assertFalse(condition);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertFalse(TestContext context, String message, boolean condition) {
        try {
            Assert.assertFalse(message, condition);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertTrue(TestContext context, boolean condition) {
        try {
            Assert.assertTrue(condition);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertTrue(TestContext context, String message, boolean condition) {
        try {
            Assert.assertTrue(message, condition);
        } catch (Throwable e) {
            context.fail(e);
        }
    }


    public static void assertArrayEquals(TestContext context, byte[] expected, byte[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, byte[] expected, byte[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, int[] expected, int[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, int[] expected, int[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, long[] expected, long[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, long[] expected, long[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, short[] expected, short[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, short[] expected, short[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, float[] expected, float[] actual, float delta) {
        try {
            Assert.assertArrayEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, float[] expected, float[] actual, float delta) {
        try {
            Assert.assertArrayEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, double[] expected, double[] actual, double delta) {
        try {
            Assert.assertArrayEquals(expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, double[] expected, double[] actual, double delta) {
        try {
            Assert.assertArrayEquals(message, expected, actual, delta);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, char[] expected, char[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, char[] expected, char[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, Object[] expected, Object[] actual) {
        try {
            Assert.assertArrayEquals(expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }

    public static void assertArrayEquals(TestContext context, String message, Object[] expected, Object[] actual) {
        try {
            Assert.assertArrayEquals(message, expected, actual);
        } catch (Throwable e) {
            context.fail(e);
        }
    }
}
