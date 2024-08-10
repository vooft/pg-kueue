package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueTopicPartition(
    val topic: KueueTopic,
    val partitionIndex: KueuePartitionIndex,
    val nextPartitionOffset: KueuePartitionOffset,
    val version: Int,
    val createdAt: Instant,
    val updatedAt: Instant
)
