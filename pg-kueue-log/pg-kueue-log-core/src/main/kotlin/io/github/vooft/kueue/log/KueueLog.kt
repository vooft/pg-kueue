package io.github.vooft.kueue.log

import io.github.vooft.kueue.KueueTopic

interface KueueLog {
    suspend fun createProducer(topic: KueueTopic): KueueProducer
    suspend fun createConsumer(topic: KueueTopic): KueueConsumer
}
