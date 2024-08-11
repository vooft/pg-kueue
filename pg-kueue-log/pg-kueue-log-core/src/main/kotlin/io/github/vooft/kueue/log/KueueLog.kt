package io.github.vooft.kueue.log

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic

interface KueueLog<C, KC: KueueConnection<C>> {
    suspend fun createProducer(topic: KueueTopic): KueueProducer<C, KC>
    suspend fun createConsumer(topic: KueueTopic): KueueConsumer
}
