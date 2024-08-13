package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

data class KueueConsumerGroupLeaderLock(
    val group: KueueConsumerGroup,
    val topic: KueueTopic,
    val consumer: KueueConsumerName,
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now(),
    val lastHeartbeat: Instant = now()
) {
    companion object {
        val MAX_HEARTBEAT_TIMEOUT = 30.seconds
        val HEARTBEAT_DELAY= 10.seconds
    }
}

fun KueueConsumerGroupLeaderLock.heartbeat(): KueueConsumerGroupLeaderLock {
    return copy(
        version = version + 1,
        lastHeartbeat = now(),
        updatedAt = now()
    )
}

fun KueueConsumerGroupLeaderLock.installLeader(consumer: KueueConsumerName): KueueConsumerGroupLeaderLock {
    return copy(
        consumer = consumer,
        version = version + 1,
        updatedAt = now(),
        lastHeartbeat = now()
    )
}
