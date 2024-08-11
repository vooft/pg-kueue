package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.generated.sql.tables.records.MessagesRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicPartitionsRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicsRecord
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
