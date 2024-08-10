package io.github.vooft.kueue.log.impl

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.KueueProducer
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueMessage
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.KueueValue
import io.github.vooft.kueue.withAcquiredConnection
import java.time.Instant

class KueueProducerImpl<C, KC : KueueConnection<C>>(
    override val topic: KueueTopic,
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>
) : KueueProducer<C, KC> {
    override suspend fun produce(key: KueueKey, value: KueueValue, existingConnection: KC?): KueueMessage {
        return connectionProvider.withAcquiredConnection(existingConnection) { connection ->
            val topicModel = persister.getOrCreateTopic(topic, connection)
            val partitionIndex = key.partition(topicModel.partitions)

            val partitionModel = persister.getOrCreatePartition(topic, partitionIndex, connection)

            val message = KueueMessage(
                topic = topic,
                partition = partitionIndex,
                offset = partitionModel.nextPartitionOffset,
                key = key,
                value = value,
                createdAt = Instant.now()
            )

            persister.incrementNextOffset(partitionModel, connection)
            persister.insertMessage(message, connection)
        }
    }
}

private fun KueueKey.partition(partitionCount: Int): KueuePartitionIndex {
    return KueuePartitionIndex(key.hashCode() % partitionCount)
}
