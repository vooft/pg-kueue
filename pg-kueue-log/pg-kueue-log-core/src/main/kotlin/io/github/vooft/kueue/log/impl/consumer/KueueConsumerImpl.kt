package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.Companion.MAX_POLL_BATCH
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.Companion.POLL_TIMEOUT
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

    private val leaderJob = coroutineScope.launch(start = CoroutineStart.LAZY) { leaderLoop() }.apply {
        invokeOnCompletion {
            logger.info(it) { "Leader job $consumerName completed" }
        }
    }
    private val pollJob = coroutineScope.launch(start = CoroutineStart.LAZY) { pollLoop() }.apply {
        invokeOnCompletion {
            logger.info(it) { "Poll job $consumerName completed" }
        }
    }

    override val messages = Channel<KueueMessageModel>()

    @Suppress("detekt:RedundantSuspendModifier")
    suspend fun init() {
        consumerDao.init(consumerGroup)
        leaderJob.start()
        pollJob.start()
    }

    override suspend fun commit(offset: KueueTopicPartitionOffset) {
        consumerDao.commitOffset(partition = offset.partitionIndex, offset = offset.partitionOffset, topic = topic, group = consumerGroup)
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
        var nextOffsets = mapOf<KueuePartitionIndex, KueuePartitionOffset>()
        var latestGroupVersion = -1
        while (isActive) {
            val consumerModel = consumerDao.heartbeat(consumerName, topic, consumerGroup)
            val groupModel = consumerDao.getGroup(consumerGroup)

            if (latestGroupVersion != groupModel.version) {
                // rebalance happened, need to resubscribe to partitions and query offsets
                logger.info { "Group $consumerGroup was rebalanced, old=$latestGroupVersion, new=${groupModel.version}" }

                latestGroupVersion = groupModel.version

                val assignedPartitions = consumerModel.assignedPartitions

                // subscribe to new partitions
                val newPartitions = assignedPartitions - nextOffsets.keys
                newPartitions.forEach { poller.subscribe(topic, it) }

                // unsubscribe from old partitions
                val oldPartitions = nextOffsets.keys - assignedPartitions
                oldPartitions.forEach { poller.unsubscribe(topic, it) }

                nextOffsets = consumerDao.queryCommittedOffsets(consumerModel.assignedPartitions, topic, consumerGroup)
            }

            val messagesPerPartition = nextOffsets.map { (partition, offset) ->
                partition to poller.poll(topic, partition, offset, MAX_POLL_BATCH)
            }.toMap()

            val receivedMessages = messagesPerPartition.flatMap { it.value }

            // TODO: send in a background job and only update heartbeat, without querying next messages, while running
            for (receivedMessage in receivedMessages) {
                messages.send(receivedMessage)
            }

            nextOffsets = nextOffsets + messagesPerPartition.map { (partition, messages) ->
                partition to (messages.maxBy { it.partitionOffset.offset }.partitionOffset + 1)
            }

            // delay only if we read all the remaining messages
            if (messagesPerPartition.values.all { it.size < MAX_POLL_BATCH }) {
                delay(POLL_TIMEOUT)
            }
        }
    }

    companion object : LoggerHolder()
}
