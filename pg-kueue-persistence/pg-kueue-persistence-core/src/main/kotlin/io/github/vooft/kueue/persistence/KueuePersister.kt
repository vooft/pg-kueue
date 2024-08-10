package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic

interface KueuePersister<C, KC : KueueConnection<C>> {
    suspend fun saveMessage(topic: String, message: String, kueueConnection: KC)

    suspend fun getOrCreateTopic(topic: KueueTopic, kueueConnection: KC): KueueTopicModel
    suspend fun getOrCreatePartition(topic: KueueTopic, partitionIndex: KueuePartitionIndex, kueueConnection: KC): KueueTopicPartition
    suspend fun incrementNextOffset(topicPartition: KueueTopicPartition, kueueConnection: KC): KueueTopicPartition
    suspend fun insertMessage(message: KueueMessage, kueueConnection: KC): KueueMessage
}
