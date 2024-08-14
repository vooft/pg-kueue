package io.github.vooft.kueue.test

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.vooft.kueue.IntegrationTest
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.jdbc.JdbcDataSourceKueueConnectionProvider
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerDao
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerImpl
import io.github.vooft.kueue.log.impl.poller.PersisterKueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.minutes

class KueueConsumerTest : IntegrationTest() {

    private lateinit var dataSource: HikariDataSource

    private val topic = KueueTopic(UUID.randomUUID().toString())
    private val group = KueueConsumerGroup(UUID.randomUUID().toString())

    @BeforeEach
    fun setUp() {
        dataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = psql.jdbcUrl
                username = psql.username
                password = psql.password
            }
        )

        Flyway.configure()
            .dataSource(psql.jdbcUrl, psql.username, psql.password)
            .locations("classpath:kueue-database")
            .load()
            .migrate()

        psql.createConnection("").use {
            it.createStatement().execute("TRUNCATE topics CASCADE")
        }

        psql.createConnection("").use {
            it.createStatement().execute("INSERT INTO topics (name, partitions, created_at) VALUES ('${topic.topic}', 1, now())")
        }
    }

    @AfterEach
    fun tearDown() {
        dataSource.close()
    }

    @Test
    fun `should elect leader`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumer = KueueConsumerImpl(
            topic = topic,
            consumerGroup = group,
            consumerDao = KueueConsumerDao(
                connectionProvider = connectionProvider,
                persister = JdbcKueuePersister(),
            ),
            poller = PersisterKueueConsumerMessagePoller(
                connectionProvider = connectionProvider,
                persister = JdbcKueuePersister()
            )
        )

        consumer.init()

        eventually(1.minutes) {
            dataSource.connection.use { connection ->
                val leader = JdbcKueuePersister()
                    .findConsumerGroupLeaderLock(topic, group, connection)
                    .shouldNotBeNull()

                leader.consumer shouldBe consumer.consumerName
            }
        }
    }

    @Test
    fun `should assign partitions to multiple consumers`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val partitions = 10
        val topic2 = KueueTopic(UUID.randomUUID().toString())
        psql.createConnection("").use {
            it.createStatement().execute("INSERT INTO topics (name, partitions, created_at) VALUES ('${topic2.topic}', $partitions, now())")
        }

        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumers = List(partitions) {
            KueueConsumerImpl(
                topic = topic2,
                consumerGroup = group,
                consumerName = KueueConsumerName(it.toString()),
                consumerDao = KueueConsumerDao(
                    connectionProvider = connectionProvider,
                    persister = JdbcKueuePersister()
                ),
                poller = PersisterKueueConsumerMessagePoller(
                    connectionProvider = connectionProvider,
                    persister = JdbcKueuePersister()
                )
            )
        }

        consumers.forEach { it.init() }

        eventually(1.minutes) {
            dataSource.connection.use { connection ->
                val connectedConsumers = JdbcKueuePersister()
                    .findConnectedConsumers(topic2, group, connection)

                connectedConsumers.size shouldBe partitions

                val assignedPartitions = connectedConsumers.flatMap { it.assignedPartitions }.map { it.index }.sorted()
                assignedPartitions shouldContainExactly (0 until partitions).toList()
            }
        }
    }

    companion object : LoggerHolder()
}
