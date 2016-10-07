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

import com.google.common.base.Ascii;

public class NullSafeAscii {

    public static String toLowerCase(String string) {
        if (string != null) {
            return Ascii.toLowerCase(string);
        }
        return null;
    }

    public static String toLowerCase(CharSequence chars) {
        if (chars != null) {
            return Ascii.toLowerCase(chars);
        }
        return null;
    }

    public static char toLowerCase(char c) {
        return Ascii.toUpperCase(c);
    }

    public static String toUpperCase(String string) {
        if (string != null) {
            return Ascii.toUpperCase(string);
        }
        return null;
    }

    public static String toUpperCase(CharSequence chars) {
        if (chars != null) {
            return Ascii.toUpperCase(chars);
        }
        return null;
    }

    public static char toUpperCase(char c) {
        return Ascii.toUpperCase(c);
    }

    public static boolean isLowerCase(char c) {
        return Ascii.isLowerCase(c);
    }

    public static boolean isUpperCase(char c) {
        return Ascii.isUpperCase(c);
    }

    public static String truncate(CharSequence seq, int maxLength, String truncationIndicator) {
        if (seq != null) {
            return Ascii.truncate(seq, maxLength, truncationIndicator);
        }
        return null;
    }

    public static boolean equalsIgnoreCase(CharSequence s1, CharSequence s2) {
        if (s1 == s2) {
            return true;
        } else if (s1 == null || s2 == null) {
            return false;
        } else {
            return Ascii.equalsIgnoreCase(s1, s2);
        }
    }
}

