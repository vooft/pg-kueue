package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueConsumerGroupModel(
    val name: KueueConsumerGroup,
    val topic: KueueTopic,
    val status: KueueConsumerGroupStatus,
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now()
) {
    enum class KueueConsumerGroupStatus {
        BALANCED,
        REBALANCING
    }
}

fun KueueConsumerGroupModel.markRebalancing(): KueueConsumerGroupModel {
    check(status == KueueConsumerGroupModel.KueueConsumerGroupStatus.BALANCED) { "Consumer group is not balanced" }
    return copy(status = KueueConsumerGroupModel.KueueConsumerGroupStatus.REBALANCING, version = version + 1, updatedAt = now())
}

fun KueueConsumerGroupModel.markBalanced(): KueueConsumerGroupModel {
    check(status == KueueConsumerGroupModel.KueueConsumerGroupStatus.REBALANCING) { "Consumer group is not rebalancing" }
    return copy(status = KueueConsumerGroupModel.KueueConsumerGroupStatus.BALANCED, version = version + 1, updatedAt = now())
}
