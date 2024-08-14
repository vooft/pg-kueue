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
import io.github.vooft.kueue.persistence.KueueTopicPartitionOffset
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.ReceiveChannel
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

    private val leaderJob = coroutineScope.launch(start = CoroutineStart.LAZY) { leaderLoop() }.apply {
        invokeOnCompletion { logger.info(it) { "Leader loop completed for consumer=$consumerName topic=$topic, group=$consumerGroup" } }
    }

    override val messages: ReceiveChannel<KueueMessageModel>
        get() = TODO("Not yet implemented")

    suspend fun init() {
        consumerService.connectConsumer(consumerName, topic, consumerGroup)

        leaderJob.start()
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

    override suspend fun commit(offset: KueueTopicPartitionOffset) {
        TODO("Not yet implemented")
    }

    override suspend fun close() {
        TODO("Not yet implemented")
    }

    companion object : LoggerHolder()
}
