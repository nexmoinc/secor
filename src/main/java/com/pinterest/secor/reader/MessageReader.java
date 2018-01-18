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
package com.pinterest.secor.reader;

import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import org.apache.kafka.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.timestamp.KafkaMessageTimestampFactory;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.RateLimitUtil;
import com.pinterest.secor.util.StatsUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Message reader consumer raw Kafka messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageReader {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);

    protected SecorConfig mConfig;
    protected OffsetTracker mOffsetTracker;
    protected KafkaConsumer<String, String> mConsumer;
    protected HashMap<TopicPartition, Long> mLastAccessTime;
    protected final int mTopicPartitionForgetSeconds;
    protected final int mCheckMessagesPerSecond;
    protected int mNMessages;
    protected ConsumerRecords<String, String> mRecords = null;
    protected Iterator<ConsumerRecord<String, String>> mRecordsIterator = null;
    protected KafkaMessageTimestampFactory mKafkaMessageTimestampFactory;

    public MessageReader(SecorConfig config, OffsetTracker offsetTracker) throws
            UnknownHostException {
        mConfig = config;
        mOffsetTracker = offsetTracker;

        mConsumer = new KafkaConsumer<String, String>(mConfig.createKafkaConsumerConfig());

        if (!mConfig.getKafkaTopicBlacklist().isEmpty()) {
            throw new RuntimeException("Topic blacklist is not supported.");
        }
        mConsumer.subscribe(Pattern.compile(mConfig.getKafkaTopicFilter()));

        mLastAccessTime = new HashMap<TopicPartition, Long>();
        StatsUtil.setLabel("secor.kafka.consumer.id", IdUtil.getConsumerId());
        mTopicPartitionForgetSeconds = mConfig.getTopicPartitionForgetSeconds();
        mCheckMessagesPerSecond = mConfig.getMessagesPerSecond() / mConfig.getConsumerThreads();
        mKafkaMessageTimestampFactory = new KafkaMessageTimestampFactory(mConfig.getKafkaMessageTimestampClass());
    }

    private void updateAccessTime(TopicPartition topicPartition) {
        long now = System.currentTimeMillis() / 1000L;
        mLastAccessTime.put(topicPartition, now);
        Iterator iterator = mLastAccessTime.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pair = (Map.Entry) iterator.next();
            long lastAccessTime = (Long) pair.getValue();
            if (now - lastAccessTime > mTopicPartitionForgetSeconds) {
                iterator.remove();
            }
        }
    }

    private void exportStats() {
        StringBuffer topicPartitions = new StringBuffer();
        for (TopicPartition topicPartition : mLastAccessTime.keySet()) {
            if (topicPartitions.length() > 0) {
                topicPartitions.append(' ');
            }
            topicPartitions.append(topicPartition.topic() + '/' +
                                   topicPartition.partition());
        }
        StatsUtil.setLabel("secor.topic_partitions", topicPartitions.toString());
    }


    public KafkaConsumer<String, String> getKafkaConsumer() {
        return mConsumer;
    }

    public boolean hasNext() throws RetryLaterException {
        if ((mRecords == null) || (mRecordsIterator == null) || (!mRecordsIterator.hasNext())) {
            mRecords = mConsumer.poll(mConfig.getConsumerTimeoutMs());
            mRecordsIterator = mRecords.iterator();
        }
        if (!mRecordsIterator.hasNext()) {
            throw new RetryLaterException("No message for now, retry in a bit");
        }
        return (mRecordsIterator != null);
    }

    public Message read() throws RetryLaterException {
        assert hasNext();
        mNMessages = (mNMessages + 1) % mCheckMessagesPerSecond;
        if (mNMessages % mCheckMessagesPerSecond == 0) {
            RateLimitUtil.acquire(mCheckMessagesPerSecond);
        }
        ConsumerRecord<String, String> record;

        record = mRecordsIterator.next();
        long timestamp = (mConfig.useKafkaTimestamp())
                ? record.timestamp()
                : 0l;
        if (record.value()==null) {
            LOG.info("Some record is null in topic {}, partition {}, offset {}",
                    record.topic(), record.partition(), record.offset());
        }
        Message message = new Message(record.topic(), record.partition(),
                                      record.offset(), record.key()==null?null:record.key().getBytes(),
                                      record.value()==null?null:record.value().getBytes(), timestamp);
        TopicPartition topicPartition = new TopicPartition(message.getTopic(),
                                                           message.getKafkaPartition());
        updateAccessTime(topicPartition);
        // Skip already committed messages.
        long committedOffsetCount = mOffsetTracker.getTrueCommittedOffsetCount(topicPartition);
        LOG.debug("read message {}", message);
        if (mNMessages % mCheckMessagesPerSecond == 0) {
            exportStats();
        }
        if (message.getOffset() < committedOffsetCount) {
            LOG.debug("skipping message {} because its offset precedes committed offset count {}",
                    message, committedOffsetCount);
            return null;
        }
        return message;
    }
}
