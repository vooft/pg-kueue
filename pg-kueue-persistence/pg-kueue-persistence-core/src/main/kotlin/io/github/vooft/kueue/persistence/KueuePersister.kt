package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic

@Suppress("detekt:TooManyFunctions")
interface KueuePersister<C, KC : KueueConnection<C>> {
    suspend fun getTopic(topic: KueueTopic, connection: C): KueueTopicModel
    suspend fun findGroup(group: KueueConsumerGroup, connection: C): KueueConsumerGroupModel?
    suspend fun findTopicPartition(topic: KueueTopic, partitionIndex: KueuePartitionIndex, connection: C): KueueTopicPartitionModel?
    suspend fun findConsumerGroupLeaderLock(topic: KueueTopic, group: KueueConsumerGroup, connection: C): KueueConsumerGroupLeaderLock?

    suspend fun findConnectedConsumers(topic: KueueTopic, group: KueueConsumerGroup, connection: C): List<KueueConnectedConsumerModel>

    suspend fun findCommittedOffset(
        group: KueueConsumerGroup,
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        connection: C
    ): KueueCommittedOffsetModel?

    suspend fun getMessages(
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        firstOffset: KueuePartitionOffset,
        lastOffset: KueuePartitionOffset = firstOffset,
        connection: C
    ): List<KueueMessageModel>

    suspend fun upsert(model: KueueTopicModel, connection: C): KueueTopicModel
    suspend fun upsert(model: KueueTopicPartitionModel, connection: C): KueueTopicPartitionModel
    suspend fun upsert(model: KueueMessageModel, connection: C): KueueMessageModel
    suspend fun upsert(model: KueueConsumerGroupModel, connection: C): KueueConsumerGroupModel
    suspend fun upsert(model: KueueConsumerGroupLeaderLock, connection: C): KueueConsumerGroupLeaderLock
    suspend fun upsert(model: KueueConnectedConsumerModel, connection: C): KueueConnectedConsumerModel
    suspend fun upsert(model: KueueCommittedOffsetModel, connection: C): KueueCommittedOffsetModel

    suspend fun delete(model: KueueConnectedConsumerModel, connection: C)

    // TODO: move to a better place
    suspend fun <T> withTransaction(kueueConnection: KC, block: suspend (C) -> T): T
}

suspend fun <C, KC : KueueConnection<C>> KueuePersister<C, KC>.findConnectedConsumer(
    consumerName: KueueConsumerName,
    topic: KueueTopic,
    group: KueueConsumerGroup,
    connection: C
): KueueConnectedConsumerModel? = findConnectedConsumers(topic, group, connection).find {
    it.consumerName == consumerName
}

suspend fun <C, KC : KueueConnection<C>> KueuePersister<C, KC>.getOrCreateGroup(
    group: KueueConsumerGroup,
    connection: C
): KueueConsumerGroupModel = findGroup(group, connection) ?: upsert(KueueConsumerGroupModel(name = group), connection)
