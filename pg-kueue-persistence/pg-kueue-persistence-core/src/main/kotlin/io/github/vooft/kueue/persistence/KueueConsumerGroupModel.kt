package io.github.vooft.kueue.persistence

import java.time.Instant

data class KueueConsumerGroupModel(
    val name: KueueConsumerGroup,
    val version: Int = 1,
    val createdAt: Instant = now(),
    val updatedAt: Instant = now()
)

fun KueueConsumerGroupModel.bump() = copy(
    version = version + 1,
    updatedAt = Instant.now()
)
