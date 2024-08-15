package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

data class KueueConnectedConsumerModel(
    val consumerName: KueueConsumerName,
    val groupName: KueueConsumerGroup,
    val topic: KueueTopic,
    val status: KueueConnectedConsumerStatus,
    val assignedPartitions: Set<KueuePartitionIndex> = setOf(),
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now(),
    val lastHeartbeat: Instant = now()
) {
    enum class KueueConnectedConsumerStatus {
        BALANCED,
        UNBALANCED
    }

    companion object {
        val POLL_TIMEOUT = 1.seconds
        const val MAX_POLL_BATCH = 10
    }
}

fun KueueConnectedConsumerModel.balance(partitions: Set<KueuePartitionIndex>): KueueConnectedConsumerModel = copy(
    assignedPartitions = partitions,
    status = KueueConnectedConsumerModel.KueueConnectedConsumerStatus.BALANCED,
    version = version + 1,
    updatedAt = now()
)

fun KueueConnectedConsumerModel.heartbeat(): KueueConnectedConsumerModel = copy(
    version = version + 1,
    lastHeartbeat = now(),
    updatedAt = now()
)
