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
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * JsonDoubleTimestampedMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from JSON data and partitions data by year/month/day/hour + writing date.
 */
public class JsonDoubleTimestampedMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonDoubleTimestampedMessageParser.class);
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
    protected final SimpleDateFormat mIso8601Parser;
    protected final SimpleDateFormat mIso8601ParserMs;

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

        mIso8601Parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        mIso8601Parser.setTimeZone(config.getTimeZone());

        mIso8601ParserMs = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        mIso8601ParserMs.setTimeZone(config.getTimeZone());
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
                try {
                    return toMillis(Double.valueOf(fieldValue.toString()).longValue());
                } catch (NumberFormatException nfe) {
                    try {
                        return toMillis(mIso8601Parser.parse(fieldValue.toString()).getTime());
                    } catch (ParseException pe) {}
                    try {
                        return toMillis(mIso8601ParserMs.parse(fieldValue.toString()).getTime());
                    } catch (ParseException pe) {}
                    LOG.error("Unable to extract date from " + fieldValue.toString());
                }
            }
        } else if (m_timestampRequired) {
            throw new RuntimeException("Missing timestamp field for message: " + message);
        }
        return 0;
    }

    public static void main(String[] args) {
        System.setProperty("config", "src/main/config/secor.dev.properties");
        try {
            SecorConfig secorConfig = SecorConfig.load();
            JsonDoubleTimestampedMessageParser messageParser = new JsonDoubleTimestampedMessageParser(secorConfig);

            String[] jsonStrings = {
                    "{\"timestamp\":\"1484956817\"}",
                    "{\"timestamp\":\"1484953217672\"}",
                    "{\"timestamp\":\"1484949617.409\"}",
                    "{\"timestamp\":\"2017-01-17T13:48:32.564Z\"}",
                    "{\"timestamp\":\"2017-01-17T13:48:32+00:00\"}"
            };

            long timestamp = System.currentTimeMillis();
            for (String json: jsonStrings) {
                Message message = new Message("TestTopic", 0, 0L, null, json.getBytes(), timestamp);
                try {
                    System.out.println("Result:");
                    System.out.println("- Message: " + json);
                    System.out.print("- Path: ");
                    for (String path: messageParser.generatePartitions(messageParser.extractTimestampMillis(message), false, false)) {
                        System.out.print(path+"/");
                    }
                    System.out.println();
                } catch (Exception ex) {
                    System.err.println("Exception during parsing");
                    ex.printStackTrace();
                }
            }
        } catch (ConfigurationException ce) {
            System.err.println("Configuration exception");
            ce.printStackTrace();
        }
    }
}
