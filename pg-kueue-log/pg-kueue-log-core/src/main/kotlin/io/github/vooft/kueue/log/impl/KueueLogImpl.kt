package io.github.vooft.kueue.log.impl

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.log.KueueLog
import io.github.vooft.kueue.log.KueueProducer
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerDao
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerImpl
import io.github.vooft.kueue.log.impl.producer.KueueProducerImpl
import io.github.vooft.kueue.log.impl.producer.withConnection
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.KueueTopicModel
import io.github.vooft.kueue.persistence.KueueTopicPartitionModel
import java.time.Instant

class KueueLogImpl<C, KC : KueueConnection<C>>(
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>,
    private val poller: KueueConsumerMessagePoller
) : KueueLog<C, KC> {

    override suspend fun createTopic(topic: KueueTopic, partitions: Int) {
        connectionProvider.withConnection { kc ->
            persister.withTransaction(kc) { c ->
                persister.upsert(KueueTopicModel(name = topic, partitions = partitions, createdAt = Instant.now()), c)
                repeat(partitions) {
                    persister.upsert(
                        model = KueueTopicPartitionModel(
                            topic = topic,
                            partitionIndex = KueuePartitionIndex(it),
                            nextPartitionOffset = KueuePartitionOffset(1)
                        ),
                        connection = c
                    )
                }
            }
        }
    }

    override suspend fun createProducer(topic: KueueTopic): KueueProducer<C, KC> =
        KueueProducerImpl(topic = topic, connectionProvider = connectionProvider, persister = persister)

    override suspend fun createConsumer(topic: KueueTopic, group: KueueConsumerGroup): KueueConsumer = KueueConsumerImpl(
        topic = topic,
        consumerGroup = group,
        consumerDao = KueueConsumerDao(connectionProvider, persister),
        poller = poller
    ).also { it.init() }
}
