package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueConnectedConsumerModel(
    val consumerName: KueueConsumerName,
    val groupName: KueueConsumerGroup,
    val topic: KueueTopic,
    val assignedPartitions: Set<KueuePartitionIndex> = setOf(),
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now(),
    val lastHeartbeat: Instant = now()
)

fun KueueConnectedConsumerModel.assignPartitions(partitions: Set<KueuePartitionIndex>): KueueConnectedConsumerModel {
    return copy(
        assignedPartitions = partitions,
        version = version + 1,
        updatedAt = now()
    )
}
