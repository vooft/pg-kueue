package io.github.vooft.kueue.log

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueueTopicPartitionOffset
import kotlinx.coroutines.channels.ReceiveChannel

interface KueueConsumer {
    val topic: KueueTopic
    val messages: ReceiveChannel<KueueMessageModel>
    suspend fun commit(offset: KueueTopicPartitionOffset)
    suspend fun close()
}
