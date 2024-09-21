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
import io.github.vooft.kueue.log.impl.producer.KueueProducerImpl
import io.github.vooft.kueue.persistence.KueueConnectedConsumerModel.KueueConnectedConsumerStatus.UNBALANCED
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.KueueConsumerName
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueMessageModel
import io.github.vooft.kueue.persistence.KueueValue
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.withClue
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import javax.sql.DataSource
import kotlin.time.Duration.Companion.minutes

class KueueConsumerTest : IntegrationTest() {

    private lateinit var hikariDataSource: HikariDataSource
    private lateinit var dataSource: DataSource

    private val singlePartitionTopic = KueueTopic(UUID.randomUUID().toString())
    private val group = KueueConsumerGroup(UUID.randomUUID().toString())

    private val multiplePartitions = 10
    private val multiplePartitionTopic = KueueTopic(UUID.randomUUID().toString())

    @BeforeEach
    fun setUp() {
        hikariDataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = psql.jdbcUrl
                username = psql.username
                password = psql.password
            }
        )

//        dataSource = ProxyDataSourceBuilder.create(hikariDataSource).traceMethods().build()
        dataSource = hikariDataSource

        Flyway.configure()
            .dataSource(psql.jdbcUrl, psql.username, psql.password)
            .locations("classpath:kueue-database")
            .load()
            .migrate()

        psql.createConnection("").use {
            it.createStatement().execute("TRUNCATE topics CASCADE")
        }

        psql.createConnection("").use {
            it.createStatement().execute(
                "INSERT INTO topics (name, partitions, created_at) VALUES ('${singlePartitionTopic.topic}', 1, now())"
            )
        }

        psql.createConnection("").use {
            it.createStatement().execute(
                "INSERT INTO topics (name, partitions, created_at) " +
                    "VALUES ('${multiplePartitionTopic.topic}', $multiplePartitions, now())"
            )
        }
    }

    @AfterEach
    fun tearDown() {
        hikariDataSource.close()
    }

    @Test
    fun `should elect leader`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumer = KueueConsumerImpl(
            topic = singlePartitionTopic,
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

        try {
            consumer.init()

            eventually(1.minutes) {
                dataSource.connection.use { connection ->
                    val leader = JdbcKueuePersister()
                        .findConsumerGroupLeaderLock(singlePartitionTopic, group, connection)
                        .shouldNotBeNull()

                    leader.consumer shouldBe consumer.consumerName
                }
            }
        } finally {
            consumer.close()
        }
    }

    @Test
    fun `should assign partitions to multiple consumers`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumers = List(multiplePartitions) {
            KueueConsumerImpl(
                topic = multiplePartitionTopic,
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

        try {
            consumers.forEach { it.init() }

            eventually(1.minutes) {
                dataSource.connection.use { connection ->
                    val connectedConsumers = JdbcKueuePersister()
                        .findConnectedConsumers(multiplePartitionTopic, group, connection)

                    connectedConsumers.size shouldBe multiplePartitions

                    val assignedPartitions = connectedConsumers.flatMap { it.assignedPartitions }.map { it.index }.sorted()
                    assignedPartitions shouldContainExactly (0 until multiplePartitions).toList()
                }
            }
        } finally {
            consumers.forEach { it.close() }
        }
    }

    @Test
    fun `should consume messages`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val messagesCount = 100
        val consumersCount = 10
        val producer = KueueProducerImpl(
            topic = multiplePartitionTopic,
            connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource),
            persister = JdbcKueuePersister()
        )

        val messages = List(messagesCount) { producer.produce(KueueKey(it.toString()), KueueValue(it.toString())) }

        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumers = List(consumersCount) {
            KueueConsumerImpl(
                topic = multiplePartitionTopic,
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

        val consumedMutex = Mutex()
        val consumed = mutableListOf<KueueMessageModel>()

        try {
            consumers.map { launch { it.init() } }.joinAll()

            eventually(1.minutes) {
                val consumerModels = psql.createConnection("").use {
                    JdbcKueuePersister().findConnectedConsumers(multiplePartitionTopic, group, it)
                }

                consumerModels shouldHaveSize consumersCount
                withClue("All consumers should be balanced") { consumerModels.filter { it.status == UNBALANCED }.shouldBeEmpty() }
            }

            coroutineScope {
                val jobs = consumers.map { consumer ->
                    launch {
                        for (message in consumer.messages) {
                            println("received $message")
                            consumedMutex.withLock { consumed.add(message) }
                        }
                    }
                }

                eventually(1.minutes) {
                    val currentlyConsumed = consumedMutex.withLock { consumed.toSet() }
                    currentlyConsumed shouldContainExactlyInAnyOrder messages
                }

                jobs.forEach { it.cancel() }
            }
        } finally {
            consumers.forEach { it.close() }
        }
    }

    companion object : LoggerHolder()
}
