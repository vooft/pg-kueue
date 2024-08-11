package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant
import java.util.UUID

data class KueueMessageModel(
    val id: UUID = UUID.randomUUID(),
    override val topic: KueueTopic,
    override val partitionIndex: KueuePartitionIndex,
    override val partitionOffset: KueuePartitionOffset,
    val key: KueueKey,
    val value: KueueValue,
    val createdAt: Instant = now()
): KueueTopicPartitionOffset


