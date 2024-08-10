package io.github.vooft.kueue.persistence

import io.github.vooft.kueue.KueueTopic
import java.time.Instant

data class KueueTopicModel(val name: KueueTopic, val partitions: Int, val createdAt: Instant)
