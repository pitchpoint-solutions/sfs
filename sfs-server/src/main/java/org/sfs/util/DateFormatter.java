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


import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static java.time.ZonedDateTime.parse;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.GregorianCalendar.from;
import static java.util.Locale.US;
import static java.util.TimeZone.getTimeZone;

public class DateFormatter {

    private static final Locale LOCALE_US = US;
    private static final TimeZone TIME_ZONE = getTimeZone("UTC");
    private static final DateTimeFormatter DATE_TIME_UTC =
            ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", LOCALE_US)
                    .withZone(TIME_ZONE.toZoneId());
    private static final DateTimeFormatter RFC1123_PATTERN =
            ofPattern("EEE, dd MMM yyyy HH:mm:ss z", LOCALE_US)
                    .withZone(TIME_ZONE.toZoneId());


    public static String toDateTimeString(Calendar calendar) {
        return toDateTimeString(calendar.getTime());
    }


    public static String toDateTimeString(Date calendar) {
        if (calendar != null) {
            return DATE_TIME_UTC.format(calendar.toInstant());
        }
        return null;
    }

    public static String toRfc1123String(Calendar calendar) {
        return toRfc1123String(calendar.getTime());
    }


    public static String toRfc1123String(Date calendar) {
        if (calendar != null) {
            return RFC1123_PATTERN.format(calendar.toInstant());
        }
        return null;
    }

    public static Calendar fromDateTimeString(String formatted) {
        if (formatted != null) {
            return from(parse(formatted, DATE_TIME_UTC));
        }
        return null;
    }
}
