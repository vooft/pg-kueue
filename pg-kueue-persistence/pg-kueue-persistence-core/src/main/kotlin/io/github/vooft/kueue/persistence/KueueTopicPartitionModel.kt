package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueTopicPartitionModel(
    val topic: KueueTopic,
    val partitionIndex: KueuePartitionIndex,
    val nextPartitionOffset: KueuePartitionOffset,
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now()
)

fun KueueTopicPartitionModel.incrementNextPartitionOffset(): KueueTopicPartitionModel = copy(
    nextPartitionOffset = nextPartitionOffset + 1,
    version = version + 1,
    updatedAt = now()
)
