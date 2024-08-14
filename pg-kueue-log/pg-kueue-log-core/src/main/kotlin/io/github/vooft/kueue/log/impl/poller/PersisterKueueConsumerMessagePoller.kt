package io.github.vooft.kueue.log.impl.poller

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.log.impl.producer.withAcquiredConnection
import io.github.vooft.kueue.persistence.KueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueuePersister

class PersisterKueueConsumerMessagePoller<C, KC : KueueConnection<C>>(
    private val connectionProvider: KueueConnectionProvider<C, KC>,
    private val persister: KueuePersister<C, KC>
) : KueueConsumerMessagePoller {

    override suspend fun subscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex) {
        // no-op
    }

    override suspend fun unsubscribe(topic: KueueTopic, partitionIndex: KueuePartitionIndex) {
        // no-op
    }

    override suspend fun poll(
        topic: KueueTopic,
        partition: KueuePartitionIndex,
        offset: KueuePartitionOffset,
        limit: Int
    ): List<KueueMessageModel> = connectionProvider.withAcquiredConnection { connection ->
        persister.getMessages(topic, partition, offset, offset + limit - 1, connection)
    }
}
