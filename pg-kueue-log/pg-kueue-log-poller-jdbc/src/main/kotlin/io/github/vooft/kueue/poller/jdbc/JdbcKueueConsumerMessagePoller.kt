package io.github.vooft.kueue.poller.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset

class JdbcKueueConsumerMessagePoller(
    private val
) : KueueConsumerMessagePoller {

    override suspend fun subscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex) {
        // no op
    }

    override suspend fun unsubscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex) {
        // no op
    }

    override suspend fun poll(
        topic: KueueTopic,
        partition: KueuePartitionIndex,
        offset: KueuePartitionOffset,
        limit: Int
    ): List<KueueMessageModel> {
        TODO()
    }
}
