package io.github.vooft.kueue.log.impl

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.log.KueueLog
import io.github.vooft.kueue.log.KueueProducer

class KueueLogImpl : KueueLog {
    override suspend fun createProducer(topic: KueueTopic): KueueProducer {
        TODO("Not yet implemented")
    }

    override suspend fun createConsumer(topic: KueueTopic): KueueConsumer {
        TODO("Not yet implemented")
    }
}
