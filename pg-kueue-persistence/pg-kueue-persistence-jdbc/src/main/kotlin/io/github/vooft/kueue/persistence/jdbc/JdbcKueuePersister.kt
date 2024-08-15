package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.OptimisticLockingException
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.withNonCancellable
import io.github.vooft.kueue.common.withVirtualThreadDispatcher
import io.github.vooft.kueue.generated.sql.tables.references.COMMITTED_OFFSETS
import io.github.vooft.kueue.generated.sql.tables.references.CONNECTED_CONSUMERS
import io.github.vooft.kueue.generated.sql.tables.references.CONSUMER_GROUPS
import io.github.vooft.kueue.generated.sql.tables.references.CONSUMER_GROUP_LEADER_LOCKS
import io.github.vooft.kueue.generated.sql.tables.references.MESSAGES
import io.github.vooft.kueue.generated.sql.tables.references.TOPICS
import io.github.vooft.kueue.generated.sql.tables.references.TOPIC_PARTITIONS
import io.github.vooft.kueue.jdbc.JdbcKueueConnection
import io.github.vooft.kueue.persistence.KueueCommittedOffsetModel
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerGroupLeaderLock
import io.github.vooft.kueue.persistence.KueueConsumerGroupModel
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.KueueTopicModel
import io.github.vooft.kueue.persistence.KueueTopicPartitionModel
import io.github.vooft.kueue.useUnwrapped
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import java.sql.Connection

@Suppress("detekt:TooManyFunctions")
class JdbcKueuePersister : KueuePersister<Connection, JdbcKueueConnection> {
    override suspend fun getTopic(topic: KueueTopic, connection: Connection): KueueTopicModel {
        logger.debug { "getTopic(): topic=$topic" }

        return connection.dsl {
            selectFrom(TOPICS)
                .where(TOPICS.NAME.eq(topic.topic))
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun findGroup(group: KueueConsumerGroup, connection: Connection): KueueConsumerGroupModel? {
        logger.debug { "findGroup(): group=$group" }

        return connection.dsl {
            selectFrom(CONSUMER_GROUPS)
                .where(CONSUMER_GROUPS.NAME.eq(group.group))
                .fetchOne()
                ?.toModel()
        }
    }

    override suspend fun findTopicPartition(
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        connection: Connection
    ): KueueTopicPartitionModel? {
        logger.debug { "findTopicPartition(): topic=$topic, partitionIndex=$partitionIndex" }

        return connection.dsl {
            selectFrom(TOPIC_PARTITIONS)
                .where(
                    TOPIC_PARTITIONS.TOPIC.eq(topic.topic),
                    TOPIC_PARTITIONS.PARTITION_INDEX.eq(partitionIndex.index)
                )
                .fetchOne()
                ?.toModel()
        }
    }

    override suspend fun findConsumerGroupLeaderLock(
        topic: KueueTopic,
        group: KueueConsumerGroup,
        connection: Connection
    ): KueueConsumerGroupLeaderLock? {
        logger.debug { "findConsumerGroupLeaderLock(): topic=$topic, group=$group" }

        return connection.dsl {
            selectFrom(CONSUMER_GROUP_LEADER_LOCKS)
                .where(
                    CONSUMER_GROUP_LEADER_LOCKS.TOPIC.eq(topic.topic)
                        .and(CONSUMER_GROUP_LEADER_LOCKS.GROUP_NAME.eq(group.group))
                )
                .fetchOne()
                ?.toModel()
        }
    }

    override suspend fun findConnectedConsumers(
        topic: KueueTopic,
        group: KueueConsumerGroup,
        connection: Connection
    ): List<KueueConnectedConsumerModel> = connection.dsl {
        selectFrom(CONNECTED_CONSUMERS)
            .where(
                CONNECTED_CONSUMERS.TOPIC.eq(topic.topic)
                    .and(CONNECTED_CONSUMERS.GROUP_NAME.eq(group.group))
            )
            .fetch()
            .map { it.toModel() }
    }

    override suspend fun findCommittedOffset(
        group: KueueConsumerGroup,
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        connection: Connection
    ): KueueCommittedOffsetModel? {
        logger.debug { "findCommittedOffset(): group=$group, topic=$topic, partitionIndex=$partitionIndex" }

        return connection.dsl {
            selectFrom(COMMITTED_OFFSETS)
                .where(
                    COMMITTED_OFFSETS.TOPIC.eq(topic.topic),
                    COMMITTED_OFFSETS.GROUP_NAME.eq(group.group),
                    COMMITTED_OFFSETS.PARTITION_INDEX.eq(partitionIndex.index)
                )
                .fetchOne()
                ?.toModel()
        }
    }

    override suspend fun getMessages(
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        firstOffset: KueuePartitionOffset,
        lastOffset: KueuePartitionOffset,
        connection: Connection
    ): List<KueueMessageModel> {
        logger.debug { "getMessages(): topic=$topic, partitionIndex=$partitionIndex, firstOffset=$firstOffset, lastOffset=$lastOffset" }

        return connection.dsl {
            selectFrom(MESSAGES)
                .where(
                    MESSAGES.TOPIC.eq(topic.topic),
                    MESSAGES.PARTITION_INDEX.eq(partitionIndex.index),
                    MESSAGES.PARTITION_OFFSET.between(firstOffset.offset, lastOffset.offset),
                )
                .orderBy(MESSAGES.CREATED_AT.asc())
                .fetch()
                .map { it.toModel() }
        }
    }

    override suspend fun upsert(model: KueueTopicModel, connection: Connection): KueueTopicModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            insertInto(TOPICS)
                .set(record)
                .onConflict(TOPICS.NAME)
                .doNothing()
                .returning()
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun upsert(model: KueueTopicPartitionModel, connection: Connection): KueueTopicPartitionModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            val inserted = insertInto(TOPIC_PARTITIONS)
                .set(record)
                .onConflict(TOPIC_PARTITIONS.TOPIC, TOPIC_PARTITIONS.PARTITION_INDEX)
                .doUpdate()
                .set(record)
                .where(TOPIC_PARTITIONS.VERSION.eq(model.version - 1))
                .execute()

            if (inserted != 1) {
                throw OptimisticLockingException(
                    "TopicPartition version conflict with topic=${model.topic}, " +
                        "partitionIndex=${model.partitionIndex} " +
                        "expected version=${model.version - 1}"
                )
            }

            selectFrom(TOPIC_PARTITIONS)
                .where(
                    TOPIC_PARTITIONS.TOPIC.eq(model.topic.topic),
                    TOPIC_PARTITIONS.PARTITION_INDEX.eq(model.partitionIndex.index)
                )
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun upsert(model: KueueMessageModel, connection: Connection): KueueMessageModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            val inserted = insertInto(MESSAGES)
                .set(record)
                .onConflict(MESSAGES.TOPIC, MESSAGES.PARTITION_INDEX, MESSAGES.PARTITION_OFFSET)
                .doNothing()
                .execute()

            if (inserted == 0) {
                throw OptimisticLockingException(
                    "Message with topic=${model.topic}, " +
                        "partitionIndex=${model.partitionIndex}, " +
                        "partitionOffset=${model.partitionOffset} " +
                        "already exists"
                )
            }

            selectFrom(MESSAGES).where(
                MESSAGES.TOPIC.eq(model.topic.topic),
                MESSAGES.PARTITION_INDEX.eq(model.partitionIndex.index),
                MESSAGES.PARTITION_OFFSET.eq(model.partitionOffset.offset)
            ).fetchSingle().toModel()
        }
    }

    override suspend fun upsert(model: KueueConsumerGroupModel, connection: Connection): KueueConsumerGroupModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            val inserted = insertInto(CONSUMER_GROUPS)
                .set(record)
                .onConflict(CONSUMER_GROUPS.NAME)
                .doUpdate()
                .set(record)
                .where(CONSUMER_GROUPS.VERSION.eq(model.version - 1))
                .execute()

            if (inserted == 0) {
                throw OptimisticLockingException(
                    "ConsumerGroup version conflict with groupName=${model.name} " +
                        "expected version=${model.version - 1}"
                )
            }

            selectFrom(CONSUMER_GROUPS)
                .where(CONSUMER_GROUPS.NAME.eq(model.name.group))
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun upsert(model: KueueConsumerGroupLeaderLock, connection: Connection): KueueConsumerGroupLeaderLock {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            val inserted = insertInto(CONSUMER_GROUP_LEADER_LOCKS)
                .set(record)
                .onConflict(CONSUMER_GROUP_LEADER_LOCKS.GROUP_NAME, CONSUMER_GROUP_LEADER_LOCKS.TOPIC)
                .doUpdate()
                .set(record)
                .where(CONSUMER_GROUP_LEADER_LOCKS.VERSION.eq(model.version - 1))
                .execute()

            if (inserted == 0) {
                throw OptimisticLockingException(
                    "ConsumerGroupLeaderLock version conflict with groupName=${model.group.group}, " +
                        "topic=${model.topic.topic}, consumerName=${model.consumer.name} " +
                        "expected version=${model.version - 1}"
                )
            }

            selectFrom(CONSUMER_GROUP_LEADER_LOCKS)
                .where(
                    CONSUMER_GROUP_LEADER_LOCKS.GROUP_NAME.eq(model.group.group)
                        .and(CONSUMER_GROUP_LEADER_LOCKS.TOPIC.eq(model.topic.topic))
                        .and(CONSUMER_GROUP_LEADER_LOCKS.CONSUMER_NAME.eq(model.consumer.name))
                )
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun upsert(model: KueueConnectedConsumerModel, connection: Connection): KueueConnectedConsumerModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()

            val inserted = insertInto(CONNECTED_CONSUMERS)
                .set(record)
                .onConflict(CONNECTED_CONSUMERS.CONSUMER_NAME, CONNECTED_CONSUMERS.TOPIC, CONNECTED_CONSUMERS.GROUP_NAME)
                .doUpdate()
                .set(record)
                .where(CONNECTED_CONSUMERS.VERSION.eq(model.version - 1))
                .execute()

            if (inserted == 0) {
                throw OptimisticLockingException(
                    "ConnectedConsumer version conflict with consumerName=${model.consumerName.name}, " +
                        "topic=${model.topic.topic}, groupName=${model.groupName.group} " +
                        "expected version=${model.version - 1}"
                )
            }

            selectFrom(CONNECTED_CONSUMERS)
                .where(
                    CONNECTED_CONSUMERS.CONSUMER_NAME.eq(model.consumerName.name),
                    CONNECTED_CONSUMERS.TOPIC.eq(model.topic.topic),
                    CONNECTED_CONSUMERS.GROUP_NAME.eq(model.groupName.group)
                )
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun upsert(model: KueueCommittedOffsetModel, connection: Connection): KueueCommittedOffsetModel {
        logger.debug { "Upserting $model" }

        return connection.dsl {
            val record = model.toRecord()
            val inserted = insertInto(COMMITTED_OFFSETS)
                .set(record)
                .onConflict(
                    COMMITTED_OFFSETS.TOPIC,
                    COMMITTED_OFFSETS.GROUP_NAME,
                    COMMITTED_OFFSETS.PARTITION_INDEX
                )
                .doUpdate()
                .set(record)
                .where(COMMITTED_OFFSETS.VERSION.eq(model.version - 1))
                .execute()

            if (inserted == 0) {
                throw OptimisticLockingException(
                    "CommittedOffset version conflict with group=${model.group.group}, " +
                        "topic=${model.topic.topic}, partitionIndex=${model.partitionIndex.index} " +
                        "expected version=${model.version - 1}"
                )
            }

            selectFrom(COMMITTED_OFFSETS)
                .where(
                    COMMITTED_OFFSETS.TOPIC.eq(model.topic.topic),
                    COMMITTED_OFFSETS.GROUP_NAME.eq(model.group.group),
                    COMMITTED_OFFSETS.PARTITION_INDEX.eq(model.partitionIndex.index)
                )
                .fetchSingle()
                .toModel()
        }
    }

    override suspend fun delete(model: KueueConnectedConsumerModel, connection: Connection) {
        logger.debug { "Deleting $model" }

        connection.dsl {
            deleteFrom(CONNECTED_CONSUMERS)
                .where(
                    CONNECTED_CONSUMERS.CONSUMER_NAME.eq(model.consumerName.name),
                    CONNECTED_CONSUMERS.TOPIC.eq(model.topic.topic),
                    CONNECTED_CONSUMERS.GROUP_NAME.eq(model.groupName.group),
                    CONNECTED_CONSUMERS.VERSION.eq(model.version)
                )
                .execute()
        }
    }

    override suspend fun <T> withTransaction(kueueConnection: JdbcKueueConnection, block: suspend (Connection) -> T): T =
        kueueConnection.useUnwrapped { connection ->
            withVirtualThreadDispatcher {
                val oldAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    val result = block(connection)
                    if (oldAutoCommit) {
                        withNonCancellable { connection.commit() }
                    }

                    result
                } catch (e: Exception) {
                    if (oldAutoCommit) {
                        withNonCancellable { connection.rollback() }
                    }

                    throw e
                } finally {
                    connection.autoCommit = oldAutoCommit
                }
            }
        }

    companion object : LoggerHolder()
}

private suspend fun <T> Connection.dsl(block: suspend DSLContext.() -> T): T = withVirtualThreadDispatcher {
    DSL.using(this, SQLDialect.POSTGRES).block()
}
