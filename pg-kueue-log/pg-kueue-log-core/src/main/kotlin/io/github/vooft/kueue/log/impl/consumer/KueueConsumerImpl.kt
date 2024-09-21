package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.Companion.MAX_POLL_BATCH
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.Companion.POLL_TIMEOUT
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.HEARTBEAT_DELAY
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.REBALANCE_WAIT_DELAY
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueueTopicPartitionOffset
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

class KueueConsumerImpl<C, KC : KueueConnection<C>>(
    override val topic: KueueTopic,
    override val consumerGroup: KueueConsumerGroup,
    val consumerName: KueueConsumerName = KueueConsumerName(UUID.randomUUID().toString()),
    private val consumerDao: KueueConsumerDao<C, KC>,
    private val poller: KueueConsumerMessagePoller
) : KueueConsumer {

    private val coroutineScope = CoroutineScope(SupervisorJob() + CoroutineName("consumer-$consumerName") + loggingExceptionHandler())

    private val leaderJob = coroutineScope.launch(
        start = CoroutineStart.LAZY,
        context = CoroutineName("leader-$consumerName")
    ) { leaderLoop() }.apply { invokeOnCompletion { logger.info(it) { "Leader job $consumerName completed" } } }

    private val pollJob = coroutineScope.launch(
        start = CoroutineStart.LAZY,
        context = CoroutineName("poll-$consumerName")
    ) { pollLoop() }.apply { invokeOnCompletion { logger.info(it) { "Poll job $consumerName completed" } } }

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
                logger.info { "I am not a leader $consumerName" }
                delay(HEARTBEAT_DELAY)
                continue
            }

            logger.info { "I am a leader $consumerName" }

            consumerDao.rebalanceIfNeeded(topic, consumerGroup)

            delay(HEARTBEAT_DELAY)
        }
    }

    private suspend fun pollLoop() = supervisorScope {
        val loop = ConsumerLoop(coroutineScope)
        while (isActive) {
            try {
                loop.iterate()
            } catch (e: Exception) {
                logger.error(e) { "Error in poll loop" }
                delay(POLL_TIMEOUT)
            }
        }
    }

    private inner class ConsumerLoop(coroutineScope: CoroutineScope) {
        val nextOffsets = AtomicReference(mapOf<KueuePartitionIndex, KueuePartitionOffset>())
        var latestGroupVersion = -1
        var sendJob = coroutineScope.launch { }

        suspend fun iterate() {
            val consumerModel = consumerDao.heartbeat(consumerName, topic, consumerGroup)
            if (consumerModel.status == KueueConnectedConsumerModel.KueueConnectedConsumerStatus.UNBALANCED) {
                logger.info { "Consumer $consumerName is unbalanced, waiting for rebalance" }
                delay(REBALANCE_WAIT_DELAY)
                return
            }

            val groupModel = consumerDao.getGroup(consumerGroup)

            if (latestGroupVersion != groupModel.version) {
                // rebalance happened, need to resubscribe to partitions and query offsets
                logger.info { "Group $consumerGroup was rebalanced, old=$latestGroupVersion, new=${groupModel.version}" }

                latestGroupVersion = groupModel.version

                val assignedPartitions = consumerModel.assignedPartitions

                // subscribe to new partitions
                val newPartitions = assignedPartitions - nextOffsets.get().keys
                newPartitions.forEach { poller.subscribe(topic, it) }

                // unsubscribe from old partitions
                val oldPartitions = nextOffsets.get().keys - assignedPartitions
                oldPartitions.forEach { poller.unsubscribe(topic, it) }

                nextOffsets.set(consumerDao.queryCommittedOffsets(consumerModel.assignedPartitions, topic, consumerGroup))
            }

            if (!sendJob.isActive) {
                val messagesPerPartition = nextOffsets.get().map { (partition, offset) ->
                    partition to poller.poll(topic, partition, offset, MAX_POLL_BATCH)
                }.toMap()

                sendJob = coroutineScope.launch {
                    val receivedMessages = messagesPerPartition.flatMap { it.value }

                    // TODO: send in a background job and only update heartbeat, without querying next messages, while running
                    for (receivedMessage in receivedMessages) {
                        messages.send(receivedMessage)
                        nextOffsets.getAndUpdate { it + (receivedMessage.partitionIndex to (receivedMessage.partitionOffset + 1)) }
                    }
                }

                // if we read everything on all partitions, then just wait for a whole timeout
                if (messagesPerPartition.all { it.value.size < MAX_POLL_BATCH }) {
                    delay(POLL_TIMEOUT)
                }
            } else {
                val joinJob = coroutineScope.launch { sendJob.join() }
                val monitorJob = coroutineScope.launch {
                    delay(POLL_TIMEOUT)
                    joinJob.cancel()
                }

                joinJob.join()
                monitorJob.cancel()
            }
        }
    }

    companion object : LoggerHolder()
}
