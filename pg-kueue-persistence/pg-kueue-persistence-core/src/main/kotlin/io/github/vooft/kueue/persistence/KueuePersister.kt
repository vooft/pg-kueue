package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic

interface KueuePersister<C, KC : KueueConnection<C>> {
    suspend fun getTopic(topic: KueueTopic, connection: C): KueueTopicModel
    suspend fun findTopicPartition(topic: KueueTopic, partitionIndex: KueuePartitionIndex, connection: C): KueueTopicPartitionModel?

    suspend fun getMessages(
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        firstOffset: Int,
        lastOffset: Int = firstOffset,
        connection: C
    ): List<KueueMessageModel>

    suspend fun upsert(model: KueueTopicModel, connection: C): KueueTopicModel
    suspend fun upsert(model: KueueTopicPartitionModel, connection: C): KueueTopicPartitionModel
    suspend fun upsert(model: KueueMessageModel, connection: C): KueueMessageModel
    suspend fun upsert(model: KueueConsumerGroupModel, connection: C): KueueConsumerGroupModel

    suspend fun <T> withTransaction(kueueConnection: KC, block: suspend (C) -> T): T
}
