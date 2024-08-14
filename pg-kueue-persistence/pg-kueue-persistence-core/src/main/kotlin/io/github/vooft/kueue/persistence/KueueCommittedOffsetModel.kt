package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueCommittedOffsetModel(
    val group: KueueConsumerGroup,
    val topic: KueueTopic,
    val partitionIndex: KueuePartitionIndex,
    val offset: KueuePartitionOffset,
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now()
)
