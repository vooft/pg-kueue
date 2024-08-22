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
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueuePersister

class KueueLogImpl<C, KC : KueueConnection<C>>(
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>,
    private val poller: KueueConsumerMessagePoller
) : KueueLog<C, KC> {
    override suspend fun createProducer(topic: KueueTopic): KueueProducer<C, KC> {
        return KueueProducerImpl(topic = topic, connectionProvider = connectionProvider, persister = persister)
    }

    override suspend fun createConsumer(topic: KueueTopic, group: KueueConsumerGroup): KueueConsumer {
        return KueueConsumerImpl(
            topic = topic,
            consumerGroup = group,
            consumerDao = KueueConsumerDao(connectionProvider, persister),
            poller = poller
        )
    }
}