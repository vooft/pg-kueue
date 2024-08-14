package io.github.vooft.kueue.log.impl.consumer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.log.KueueConsumer
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock.Companion.HEARTBEAT_DELAY
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueueTopicPartitionOffset
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.UUID

class KueueConsumerImpl<C, KC : KueueConnection<C>>(
    override val topic: KueueTopic,
    override val consumerGroup: KueueConsumerGroup,
    val consumerName: KueueConsumerName = KueueConsumerName(UUID.randomUUID().toString()),
    private val consumerService: KueueConsumerService<C, KC>
) : KueueConsumer {

    private val coroutineScope = CoroutineScope(SupervisorJob() + loggingExceptionHandler())

    private val leaderJob = coroutineScope.launch(start = CoroutineStart.LAZY) { leaderLoop() }

    override val messages = Channel<KueueMessageModel>()

    suspend fun init() {
        leaderJob.start()
    }

    override suspend fun commit(offset: KueueTopicPartitionOffset) {
        TODO("Not yet implemented")
    }

    override suspend fun close() {
        messages.close()
        leaderJob.cancel()
    }

    private suspend fun leaderLoop() = coroutineScope {
        while (isActive) {
            if (!consumerService.isLeader(consumerName, topic, consumerGroup)) {
                delay(HEARTBEAT_DELAY)
                continue
            }

            logger.info { "I am a leader $consumerName" }

            consumerService.rebalanceIfNeeded(topic, consumerGroup)

            delay(HEARTBEAT_DELAY)
        }
    }

    private suspend fun heartbeatLoop() = coroutineScope {
        var assignedPartitions = emptySet<KueuePartitionIndex>()
        while (isActive) {
            val consumer = consumerService.heartbeat(consumerName, topic, consumerGroup)
            if (consumer.assignedPartitions == assignedPartitions) {
                delay(HEARTBEAT_DELAY)
                continue
            }

            assignedPartitions = consumer.assignedPartitions

            // TODO: query offsets
        }
    }

    companion object : LoggerHolder()
}
