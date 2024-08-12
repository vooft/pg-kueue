package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic

@JvmInline
value class KueuePartitionIndex(val index: Int)

@JvmInline
value class KueuePartitionOffset(val offset: Int) {
    operator fun plus(i: Int) = KueuePartitionOffset(offset + i)
}

@JvmInline
value class KueueKey(val key: String)

@JvmInline
value class KueueValue(val value: String)

@JvmInline
value class KueueConsumerGroup(val group: String)

interface KueueTopicPartitionOffset {
    val topic: KueueTopic
    val partitionIndex: KueuePartitionIndex
    val partitionOffset: KueuePartitionOffset
}
