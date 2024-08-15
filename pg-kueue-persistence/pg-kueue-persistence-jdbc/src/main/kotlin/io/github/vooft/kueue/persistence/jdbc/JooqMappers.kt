@file:Suppress("detekt:TooManyFunctions")

package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.generated.sql.tables.records.CommittedOffsetsRecord
import io.github.vooft.kueue.generated.sql.tables.records.ConnectedConsumersRecord
import io.github.vooft.kueue.generated.sql.tables.records.ConsumerGroupLeaderLocksRecord
import io.github.vooft.kueue.generated.sql.tables.records.MessagesRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicPartitionsRecord
import io.github.vooft.kueue.generated.sql.tables.records.TopicsRecord
import io.github.vooft.kueue.persistence.KueueCommittedOffsetModel
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock
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

internal fun KueueConnectedConsumerModel.toRecord() = ConnectedConsumersRecord(
    consumerName = consumerName.name,
    groupName = groupName.group,
    topic = topic.topic,
    status = status.name,
    assignedPartitions = assignedPartitions.map { it.index }.toTypedArray(),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt,
    lastHeartbeat = lastHeartbeat
)

internal fun ConnectedConsumersRecord.toModel() = KueueConnectedConsumerModel(
    consumerName = KueueConsumerName(consumerName),
    groupName = KueueConsumerGroup(groupName),
    topic = KueueTopic(topic),
    status = KueueConnectedConsumerModel.KueueConnectedConsumerStatus.valueOf(status),
    assignedPartitions = assignedPartitions.filterNotNull().map { KueuePartitionIndex(it) }.toSet(),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt,
    lastHeartbeat = lastHeartbeat
)

internal fun KueueCommittedOffsetModel.toRecord() = CommittedOffsetsRecord(
    groupName = group.group,
    topic = topic.topic,
    partitionIndex = partitionIndex.index,
    partitionOffset = offset.offset,
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)

internal fun CommittedOffsetsRecord.toModel() = KueueCommittedOffsetModel(
    group = KueueConsumerGroup(groupName),
    topic = KueueTopic(topic),
    partitionIndex = KueuePartitionIndex(partitionIndex),
    offset = KueuePartitionOffset(partitionOffset),
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt
)
