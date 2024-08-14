package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.HEARTBEAT_DELAY
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueueTopicPartitionOffset
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.util.UUID

class KueueConsumerImpl<C, KC : KueueConnection<C>>(
    override val topic: KueueTopic,
    override val consumerGroup: KueueConsumerGroup,
    val consumerName: KueueConsumerName = KueueConsumerName(UUID.randomUUID().toString()),
    private val consumerDao: KueueConsumerDao<C, KC>,
    private val poller: KueueConsumerMessagePoller
) : KueueConsumer {

    private val coroutineScope = CoroutineScope(SupervisorJob() + loggingExceptionHandler())

    private val leaderJob = coroutineScope.launch(start = CoroutineStart.LAZY) { leaderLoop() }
    private val pollJob = coroutineScope.launch(start = CoroutineStart.LAZY) { pollLoop() }

    override val messages = Channel<KueueMessageModel>()

    @Suppress("detekt:RedundantSuspendModifier")
    suspend fun init() {
        leaderJob.start()
        pollJob.start()
    }

    override suspend fun commit(offset: KueueTopicPartitionOffset) {
        TODO("Not yet implemented")
    }

    override suspend fun close() {
        pollJob.cancel()
        leaderJob.cancel()

        messages.close()

        listOf(leaderJob, pollJob).joinAll()
    }

    private suspend fun leaderLoop() = coroutineScope {
        while (isActive) {
            if (!consumerDao.isLeader(consumerName, topic, consumerGroup)) {
                delay(HEARTBEAT_DELAY)
                continue
            }

            logger.info { "I am a leader $consumerName" }

            consumerDao.rebalanceIfNeeded(topic, consumerGroup)

            delay(HEARTBEAT_DELAY)
        }
    }

    private suspend fun pollLoop() = coroutineScope {
        var consumedOffsets = mapOf<KueuePartitionIndex, KueuePartitionOffset>()
        while (isActive) {
            val consumer = consumerDao.heartbeat(consumerName, topic, consumerGroup)
            val assignedPartitions = consumer.assignedPartitions

            // TODO: change to consumer group generation?
            if (consumedOffsets.size != assignedPartitions.size || consumedOffsets.keys.containsAll(assignedPartitions)) {
                // rebalance happened, need to resubscribe to partitions and query offsets

                // subscribe to new partitions
                val newPartitions = assignedPartitions - consumedOffsets.keys
                newPartitions.forEach { poller.subscribe(topic, it) }

                // unsubscribe from old partitions
                val oldPartitions = consumedOffsets.keys - assignedPartitions
                oldPartitions.forEach { poller.unsubscribe(topic, it) }

                consumedOffsets = consumerDao.queryCommittedOffsets(consumer.assignedPartitions, topic, consumerGroup)
            }

            val messagesPerPartition = consumedOffsets.map { (partition, offset) ->
                partition to poller.poll(topic, partition, offset, MAX_POLL_BATCH)
            }.toMap()

            val receivedMessages = messagesPerPartition.flatMap { it.value }

            for (receivedMessage in receivedMessages) {
                messages.send(receivedMessage)
            }

            consumedOffsets = consumedOffsets + receivedMessages.associate { it.partitionIndex to it.partitionOffset }

            // delay only if we read all the remaining messages
            if (messagesPerPartition.values.all { it.size < MAX_POLL_BATCH }) {
                delay(HEARTBEAT_DELAY)
            }
        }
    }

    companion object : LoggerHolder()
}

private const val MAX_POLL_BATCH = 10
