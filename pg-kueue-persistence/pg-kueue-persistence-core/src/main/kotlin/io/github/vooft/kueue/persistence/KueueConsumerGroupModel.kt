package io.github.vooft.kueue.persistence

import java.time.Instant

data class KueueConsumerGroupModel(
    val name: KueueConsumerGroup,
    val status: KueueConsumerGroupStatus,
    val version: Int,
    val createdAt: Instant,
    val updatedAt: Instant
) {
    enum class KueueConsumerGroupStatus {
        BALANCED,
        REBALANCING
    }
}
