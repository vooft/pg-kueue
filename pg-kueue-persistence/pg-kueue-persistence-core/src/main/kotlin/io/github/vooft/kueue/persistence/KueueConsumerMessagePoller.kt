package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic

interface KueueConsumerMessagePoller {
    suspend fun subscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex)
    suspend fun unsubscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex)

    suspend fun poll(topic: KueueTopic, partition: KueuePartitionIndex, offset: KueuePartitionOffset, limit: Int): List<KueueMessageModel>
}
