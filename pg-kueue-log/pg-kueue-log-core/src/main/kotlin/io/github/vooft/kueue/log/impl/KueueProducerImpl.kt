package io.github.vooft.kueue.log.impl

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.KueueProducer
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.KueueTopicPartitionModel
import io.github.vooft.kueue.persistence.KueueValue
import io.github.vooft.kueue.persistence.incrementNextPartitionOffset
import io.github.vooft.kueue.retryingOptimisticLockingException

class KueueProducerImpl<C, KC : KueueConnection<C>>(
    override val topic: KueueTopic,
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>
) : KueueProducer<C, KC> {
    override suspend fun produce(key: KueueKey, value: KueueValue, existingConnection: KC?): KueueMessageModel {
        logger.debug { "Producing key=$key, value=$value" }

        val message = retryingOptimisticLockingException {

            connectionProvider.withConnection(existingConnection) { acquiredConnection ->

                logger.debug { "Acquired connection key=$key" }

                persister.withTransaction(acquiredConnection) { connection ->

                    logger.debug { "Started transaction key=$key" }

                    val topicModel = persister.getTopic(topic, connection)
                    val partitionIndex = key.partition(topicModel.partitions)

                    val partitionModel = persister.findTopicPartition(topic, partitionIndex, connection)
                        ?: KueueTopicPartitionModel(
                            topic = topic,
                            partitionIndex = partitionIndex,
                            nextPartitionOffset = KueuePartitionOffset(0)
                        )

                    persister.upsert(
                        model = partitionModel.incrementNextPartitionOffset(),
                        connection = connection
                    )

                    persister.upsert(
                        model = KueueMessageModel(
                            topic = topic,
                            partitionIndex = partitionIndex,
                            partitionOffset = partitionModel.nextPartitionOffset,
                            key = key,
                            value = value,
                        ),
                        connection = connection
                    )
                }
            }
        }


        logger.debug { "Produced key=$key, value=$value" }
        return message
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}

private fun KueueKey.partition(partitionCount: Int): KueuePartitionIndex {
    return KueuePartitionIndex(key.hashCode() % partitionCount)
}
