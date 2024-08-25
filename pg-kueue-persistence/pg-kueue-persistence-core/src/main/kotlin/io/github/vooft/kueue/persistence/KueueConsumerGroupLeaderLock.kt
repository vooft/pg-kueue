package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.MAX_HEARTBEAT_TIMEOUT
import java.time.Instant
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

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
        val HEARTBEAT_DELAY get() = Random.nextInt(1, 5).seconds
        val REBALANCE_WAIT_DELAY get() = Random.nextInt(50, 150).milliseconds
    }
}

fun KueueConsumerGroupLeaderLock.heartbeat(): KueueConsumerGroupLeaderLock = copy(
    version = version + 1,
    lastHeartbeat = now(),
    updatedAt = now()
)

fun KueueConsumerGroupLeaderLock.installLeader(consumer: KueueConsumerName): KueueConsumerGroupLeaderLock = copy(
    consumer = consumer,
    version = version + 1,
    updatedAt = now(),
    lastHeartbeat = now()
)

fun KueueConnectedConsumerModel.isStale(): Boolean = lastHeartbeat + MAX_HEARTBEAT_TIMEOUT.toJavaDuration() < Instant.now()
