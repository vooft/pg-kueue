package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.impl.producer.withAcquiredConnection
import io.github.vooft.kueue.log.impl.producer.withRetryingAcquiredConnection
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.MAX_HEARTBEAT_TIMEOUT
import io.github.vooft.kueue.persistence.KueueConsumerGroupModel
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.getConsumerGroup
import io.github.vooft.kueue.persistence.heartbeat
import io.github.vooft.kueue.persistence.installLeader
import io.github.vooft.kueue.persistence.rebalance
import io.github.vooft.kueue.swallowOptimisticLockingException
import java.time.Instant
import kotlin.time.toJavaDuration

class KueueConsumerService<C, KC : KueueConnection<C>>(
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>,
) {
    suspend fun initConsumerGroup(topic: KueueTopic, group: KueueConsumerGroup) {
        connectionProvider.withAcquiredConnection { connection ->
            val existingGroup = persister.findConsumerGroup(group, connection)
            if (existingGroup == null) {
                swallowOptimisticLockingException {
                    persister.upsert(
                        model = KueueConsumerGroupModel(
                            name = group,
                            topic = topic,
                            status = KueueConsumerGroupModel.KueueConsumerGroupStatus.REBALANCING
                        ),
                        connection = connection
                    )
                }
            }
        }
    }

    suspend fun forceRebalanceConsumerGroup(topic: KueueTopic, group: KueueConsumerGroup) {
        connectionProvider.withRetryingAcquiredConnection { connection ->
            val consumerGroup = persister.getConsumerGroup(topic, group, connection)
            if (consumerGroup.status == KueueConsumerGroupModel.KueueConsumerGroupStatus.BALANCED) {
                persister.upsert(consumerGroup.rebalance(), connection)
            }
        }
    }

    suspend fun isLeader(consumer: KueueConsumerName, topic: KueueTopic, group: KueueConsumerGroup): Boolean {
        return connectionProvider.withRetryingAcquiredConnection { connection ->
            val leaderLock = persister.findConsumerGroupLeaderLock(topic, group, connection)
            if (leaderLock == null) {
                persister.upsert(
                    model = KueueConsumerGroupLeaderLock(
                        topic = topic,
                        group = group,
                        consumer = consumer,
                    ),
                    connection = connection
                )

                return@withRetryingAcquiredConnection true
            } else {
                if (leaderLock.consumer == consumer) {
                    persister.upsert(leaderLock.heartbeat(), connection)
                    return@withRetryingAcquiredConnection true
                } else if (leaderLock.lastHeartbeat + MAX_HEARTBEAT_TIMEOUT.toJavaDuration() < Instant.now()) {
                    persister.upsert(leaderLock.installLeader(consumer), connection)
                    return@withRetryingAcquiredConnection true
                } else {
                    return@withRetryingAcquiredConnection false
                }
            }
        }
    }
}

