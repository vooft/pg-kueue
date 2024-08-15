package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.impl.producer.withAcquiredConnection
import io.github.vooft.kueue.log.impl.producer.withConnection
import io.github.vooft.kueue.log.impl.producer.withRetryingAcquiredConnection
import io.github.vooft.kueue.persistence.KueueCommittedOffsetModel
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.KueueConnectedConsumerStatus.BALANCED
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.MAX_HEARTBEAT_TIMEOUT
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.balance
import io.github.vooft.kueue.persistence.findConnectedConsumer
import io.github.vooft.kueue.persistence.heartbeat
import io.github.vooft.kueue.persistence.installLeader
import io.github.vooft.kueue.persistence.isStale
import io.github.vooft.kueue.persistence.withOffset
import io.github.vooft.kueue.retryingOptimisticLockingException
import java.time.Instant
import kotlin.time.toJavaDuration

class KueueConsumerDao<C, KC : KueueConnection<C>>(
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>,
) {
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

    suspend fun rebalanceIfNeeded(topic: KueueTopic, group: KueueConsumerGroup) {
        retryingOptimisticLockingException {
            val connectedConsumers = connectionProvider.withAcquiredConnection { persister.findConnectedConsumers(topic, group, it) }
            if (connectedConsumers.any { it.isStale() } || connectedConsumers.any { it.status != BALANCED }) {
                rebalance(topic, group)
            }
        }
    }

    suspend fun rebalance(topic: KueueTopic, group: KueueConsumerGroup) {
        retryingOptimisticLockingException {
            val (updatedConsumers, staleConsumers) = connectionProvider.withRetryingAcquiredConnection { connection ->
                val topicModel = persister.getTopic(topic, connection)
                val connectedConsumers = persister.findConnectedConsumers(topic, group, connection)

                val staleConsumers = connectedConsumers.filter { it.isStale() }
                val activeConsumers = connectedConsumers.filterNot { it.isStale() }

                val assignedPartitions = List(activeConsumers.size) { mutableSetOf<KueuePartitionIndex>() }
                repeat(topicModel.partitions) {
                    assignedPartitions[it % activeConsumers.size].add(KueuePartitionIndex(it))
                }

                activeConsumers.mapIndexed { index, consumer ->
                    consumer.balance(assignedPartitions[index])
                } to staleConsumers
            }

            connectionProvider.withConnection { acquiredConnection ->
                persister.withTransaction(acquiredConnection) { connection ->
                    updatedConsumers.forEach { persister.upsert(it, connection) }
                    staleConsumers.forEach { persister.delete(it, connection) }
                }
            }
        }
    }

    suspend fun heartbeat(consumer: KueueConsumerName, topic: KueueTopic, group: KueueConsumerGroup): KueueConnectedConsumerModel =
        connectionProvider.withRetryingAcquiredConnection { connection ->
            val consumerModel = persister.findConnectedConsumer(consumer, topic, group, connection) ?: persister.upsert(
                model = KueueConnectedConsumerModel(
                    consumerName = consumer,
                    groupName = group,
                    topic = topic,
                    status = KueueConnectedConsumerModel.KueueConnectedConsumerStatus.UNBALANCED,
                ),
                connection = connection
            )

            persister.upsert(consumerModel.heartbeat(), connection)
        }

    suspend fun queryCommittedOffsets(
        partitions: Collection<KueuePartitionIndex>,
        topic: KueueTopic,
        group: KueueConsumerGroup
    ): Map<KueuePartitionIndex, KueuePartitionOffset> = connectionProvider.withRetryingAcquiredConnection { connection ->
        partitions.associateWith { partition ->
            val offset = persister.findCommittedOffset(group, topic, partition, connection) ?: persister.upsert(
                KueueCommittedOffsetModel(
                    group = group,
                    topic = topic,
                    partitionIndex = partition,
                    offset = KueuePartitionOffset(-1),
                ),
                connection
            )

            offset.offset
        }
    }

    suspend fun commitOffset(
        partition: KueuePartitionIndex,
        offset: KueuePartitionOffset,
        topic: KueueTopic,
        group: KueueConsumerGroup
    ) {
        retryingOptimisticLockingException {
            connectionProvider.withAcquiredConnection { connection ->
                val committedOffset = persister.findCommittedOffset(group, topic, partition, connection) ?: persister.upsert(
                    model = KueueCommittedOffsetModel(
                        group = group,
                        topic = topic,
                        partitionIndex = partition,
                        offset = KueuePartitionOffset(-1),
                    ),
                    connection = connection
                )

                persister.upsert(committedOffset.withOffset(offset), connection)
            }
        }
    }
}
