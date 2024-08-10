package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueMessage(
    override val topic: KueueTopic,
    override val partition: KueuePartitionIndex,
    override val offset: KueuePartitionOffset,
    val key: KueueKey,
    val value: KueueValue,
    val createdAt: Instant
): KueueTopicPartitionOffset

interface KueueTopicPartitionOffset {
    val topic: KueueTopic
    val partition: KueuePartitionIndex
    val offset: KueuePartitionOffset
}

@JvmInline
value class KueuePartitionIndex(val partition: Int)

@JvmInline
value class KueuePartitionOffset(val offset: Int)

@JvmInline
value class KueueKey(val key: String)

@JvmInline
value class KueueValue(val value: String)
