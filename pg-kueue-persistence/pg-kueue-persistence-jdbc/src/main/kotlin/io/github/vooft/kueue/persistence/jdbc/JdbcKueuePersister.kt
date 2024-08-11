package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.OptimisticLockingException
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.withNonCancellable
import io.github.vooft.kueue.common.withVirtualThreadDispatcher
import io.github.vooft.kueue.generated.sql.tables.references.MESSAGES
import io.github.vooft.kueue.generated.sql.tables.references.TOPICS
import io.github.vooft.kueue.generated.sql.tables.references.TOPIC_PARTITIONS
import io.github.vooft.kueue.jdbc.JdbcKueueConnection
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePersister
import io.github.vooft.kueue.persistence.KueueTopicModel
import io.github.vooft.kueue.persistence.KueueTopicPartitionModel
import io.github.vooft.kueue.useUnwrapped
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import java.sql.Connection

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

    override suspend fun getMessages(
        topic: KueueTopic,
        partitionIndex: KueuePartitionIndex,
        firstOffset: Int,
        lastOffset: Int,
        connection: Connection
    ): List<KueueMessageModel> {
        logger.debug { "getMessages(): topic=$topic, partitionIndex=$partitionIndex, firstOffset=$firstOffset, lastOffset=$lastOffset" }

        return connection.dsl {
            selectFrom(MESSAGES)
                .where(
                    MESSAGES.TOPIC.eq(topic.topic),
                    MESSAGES.PARTITION_INDEX.eq(partitionIndex.index),
                    MESSAGES.PARTITION_OFFSET.between(firstOffset, lastOffset),
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
