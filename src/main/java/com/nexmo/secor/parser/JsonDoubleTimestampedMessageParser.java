/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nexmo.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.TimestampedMessageParser;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * JsonDoubleTimestampedMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from JSON data and partitions data by year/month/day/hour + writing date.
 */
public class JsonDoubleTimestampedMessageParser extends TimestampedMessageParser {
    private final boolean m_timestampRequired;

    /*
     * IMPORTANT
     * SimpleDateFormat are NOT thread-safe.
     * Each parser needs to have their own local SimpleDateFormat or it'll cause race condition.
     */
    protected final SimpleDateFormat mYearFormatter;
    protected final SimpleDateFormat mMonthFormatter;
    protected final SimpleDateFormat mDayFormatter;
    protected final SimpleDateFormat mHourFormatter;
    protected final SimpleDateFormat mWriteDayFormatter;

    public JsonDoubleTimestampedMessageParser(SecorConfig config) {
        super(config);
        m_timestampRequired = config.isMessageTimestampRequired();

        mYearFormatter = new SimpleDateFormat("yyyy");
        mYearFormatter.setTimeZone(config.getTimeZone());

        mMonthFormatter = new SimpleDateFormat("MM");
        mMonthFormatter.setTimeZone(config.getTimeZone());

        mDayFormatter = new SimpleDateFormat("dd");
        mDayFormatter.setTimeZone(config.getTimeZone());

        mHourFormatter = new SimpleDateFormat("HH");
        mHourFormatter.setTimeZone(config.getTimeZone());

        mWriteDayFormatter = new SimpleDateFormat("yyyyMMdd");
        mWriteDayFormatter.setTimeZone(config.getTimeZone());
    }

    protected String[] generatePartitions(long timestampMillis, boolean usingHourly, boolean usingMinutely)
            throws Exception {
        Date date = new Date(timestampMillis);

        return new String[]{
                mYearFormatter.format(date),
                mMonthFormatter.format(date),
                mDayFormatter.format(date),
                mHourFormatter.format(date),
                mWriteDayFormatter.format(new Date())
        };
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue = getJsonFieldValue(jsonObject);
            if (fieldValue != null) {
                return toMillis(Double.valueOf(fieldValue.toString()).longValue());
            }
        } else if (m_timestampRequired) {
            throw new RuntimeException("Missing timestamp field for message: " + message);
        }
        return 0;
    }

}
