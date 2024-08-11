package io.github.vooft.kueue.log

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueueValue

interface KueueProducer<C, KC : KueueConnection<C>> {
    val topic: KueueTopic
    suspend fun produce(key: KueueKey, value: KueueValue, existingConnection: KC? = null): KueueMessageModel
}
