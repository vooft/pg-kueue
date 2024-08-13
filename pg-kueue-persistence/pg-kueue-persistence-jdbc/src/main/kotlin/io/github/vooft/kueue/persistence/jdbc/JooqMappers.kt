package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.generated.sql.tables.records.ConsumerGroupLeaderLocksRecord
import io.github.vooft.kueue.generated.sql.tables.records.ConsumerGroupsRecord
import io.github.vooft.kueue.generated.sql.tables.records.MessagesRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicPartitionsRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicsRecord
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock
import io.github.vooft.kueue.persistence.KueueConsumerGroupModel
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueueTopicModel
import io.github.vooft.kueue.persistence.KueueTopicPartitionModel
import io.github.vooft.kueue.persistence.KueueValue

internal fun KueueMessageModel.toRecord() = MessagesRecord(
    id = id,
    topic = topic.topic,
    partitionIndex = partitionIndex.index,
    partitionOffset = partitionOffset.offset,
    key = key.key,
    message = value.value,
    createdAt = createdAt
)

internal fun MessagesRecord.toModel() = KueueMessageModel(
    id = id,
    topic = KueueTopic(topic),
    partitionIndex = KueuePartitionIndex(partitionIndex),
    partitionOffset = KueuePartitionOffset(partitionOffset),
    key = KueueKey(key),
    value = KueueValue(message),
    createdAt = createdAt
)

internal fun KueueTopicPartitionModel.toRecord() = TopicPartitionsRecord(
    topic = topic.topic,
    partitionIndex = partitionIndex.index,
    nextPartitionOffset = nextPartitionOffset.offset,
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)

internal fun TopicPartitionsRecord.toModel() = KueueTopicPartitionModel(
    topic = KueueTopic(topic),
    partitionIndex = KueuePartitionIndex(partitionIndex),
    nextPartitionOffset = KueuePartitionOffset(nextPartitionOffset),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)

internal fun KueueTopicModel.toRecord() = TopicsRecord(
    name = name.topic,
    partitions = partitions,
    createdAt = createdAt,
)

internal fun TopicsRecord.toModel() = KueueTopicModel(
    name = KueueTopic(name),
    partitions = partitions,
    createdAt = createdAt,
)

internal fun KueueConsumerGroupModel.toRecord() = ConsumerGroupsRecord(
    name = name.group,
    topic = topic.topic,
    status = status.name,
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)

internal fun ConsumerGroupsRecord.toModel() = KueueConsumerGroupModel(
    name = KueueConsumerGroup(name),
    topic = KueueTopic(topic),
    status = KueueConsumerGroupModel.KueueConsumerGroupStatus.valueOf(status),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)

internal fun KueueConsumerGroupLeaderLock.toRecord() = ConsumerGroupLeaderLocksRecord(
    groupName = group.group,
    topic = topic.topic,
    consumerName = consumer.name,
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt,
    lastHeartbeat = lastHeartbeat
)

internal fun ConsumerGroupLeaderLocksRecord.toModel() = KueueConsumerGroupLeaderLock(
    group = KueueConsumerGroup(groupName),
    topic = KueueTopic(topic),
    consumer = KueueConsumerName(consumerName),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt,
    lastHeartbeat = lastHeartbeat
)
