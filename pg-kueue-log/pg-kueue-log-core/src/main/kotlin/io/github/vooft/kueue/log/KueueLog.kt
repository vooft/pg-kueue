package io.github.vooft.kueue.log

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.persistence.KueueConsumerGroup

interface KueueLog<C, KC : KueueConnection<C>> {
    suspend fun createProducer(topic: KueueTopic): KueueProducer<C, KC>
    suspend fun createConsumer(topic: KueueTopic, group: KueueConsumerGroup): KueueConsumer

    companion object
}
