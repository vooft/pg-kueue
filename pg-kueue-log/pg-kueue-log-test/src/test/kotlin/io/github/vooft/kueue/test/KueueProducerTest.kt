package io.github.vooft.kueue.test

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.vooft.kueue.IntegrationTest
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.common.LoggerHolder
import io.github.vooft.kueue.common.loggingExceptionHandler
import io.github.vooft.kueue.jdbc.JdbcDataSourceKueueConnectionProvider
import io.github.vooft.kueue.jdbc.JdbcKueueConnection
import io.github.vooft.kueue.log.impl.producer.KueueProducerImpl
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueuePartitionIndex
import io.github.vooft.kueue.persistence.KueuePartitionOffset
import io.github.vooft.kueue.persistence.KueueValue
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import io.github.vooft.kueue.retryingOptimisticLockingException
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

class KueueProducerTest : IntegrationTest() {

    private lateinit var hikariDataSource: HikariDataSource
    private lateinit var dataSource: DataSource

    private val topic = KueueTopic(UUID.randomUUID().toString())

    @BeforeEach
    fun setUp() {
        hikariDataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = psql.jdbcUrl
                username = psql.username
                password = psql.password
            }
        )

        dataSource = ProxyDataSourceBuilder.create(hikariDataSource).traceMethods().build()

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
        hikariDataSource.close()
    }

    @Test
    fun `should produce records`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val producer = KueueProducerImpl(
            topic = topic,
            connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource),
            persister = JdbcKueuePersister()
        )

        val expectedMessages = List(10) { KueueKey(it.toString()) to KueueValue(it.toString()) }

        expectedMessages.forEach { (key, value) -> producer.produce(key, value) }

        val actualMessages = psql.createConnection("").use {
            JdbcKueuePersister().getMessages(
                topic = topic,
                partitionIndex = KueuePartitionIndex(0),
                firstOffset = KueuePartitionOffset(0),
                lastOffset = KueuePartitionOffset(expectedMessages.size + 10),
                connection = it
            )
        }.map { it.key to it.value }

        actualMessages shouldBe expectedMessages
    }

    @Test
    fun `should produce records in parallel`(): Unit = runBlocking(SupervisorJob() + Dispatchers.Default + loggingExceptionHandler()) {
        val producer = KueueProducerImpl(
            topic = topic,
            connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource),
            persister = JdbcKueuePersister()
        )

        val expectedMessages = List(1000) { KueueKey(it.toString()) to KueueValue(it.toString()) }

        val inProgress = AtomicInteger()
        expectedMessages.map { (key, value) ->
            launch {
                inProgress.incrementAndGet()
                retryingOptimisticLockingException { producer.produce(key, value) }
                inProgress.decrementAndGet()
            }
        }.joinAll()

        val actualMessages = psql.createConnection("").use {
            JdbcKueuePersister().getMessages(
                topic = topic,
                partitionIndex = KueuePartitionIndex(0),
                firstOffset = KueuePartitionOffset(0),
                lastOffset = KueuePartitionOffset(expectedMessages.size + 10),
                connection = it
            )
        }.map { it.key to it.value }

        actualMessages shouldContainExactlyInAnyOrder expectedMessages
    }

    @Test
    fun `should produce records in a single transaction`(): Unit = runBlocking(Dispatchers.Default + loggingExceptionHandler()) {
        val producer = KueueProducerImpl(
            topic = topic,
            connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource),
            persister = JdbcKueuePersister()
        )

        val expectedMessages = List(10) { KueueKey(it.toString()) to KueueValue(it.toString()) }

        dataSource.connection.use { transactionalConnection ->
            transactionalConnection.autoCommit = false
            val kueueConnection = JdbcKueueConnection(transactionalConnection)

            expectedMessages.forEach { (key, value) -> producer.produce(key, value, kueueConnection) }

            dataSource.connection.use { nonTransactionalConnection ->
                val msgs = JdbcKueuePersister().getMessages(
                    topic = topic,
                    partitionIndex = KueuePartitionIndex(0),
                    firstOffset = KueuePartitionOffset(0),
                    connection = nonTransactionalConnection
                )
                msgs shouldHaveSize 0
            }

            transactionalConnection.commit()
        }

        val actualMessages = psql.createConnection("").use {
            JdbcKueuePersister().getMessages(
                topic = topic,
                partitionIndex = KueuePartitionIndex(0),
                firstOffset = KueuePartitionOffset(0),
                lastOffset = KueuePartitionOffset(expectedMessages.size + 10),
                connection = it
            )
        }.map { it.key to it.value }

        actualMessages shouldBe expectedMessages
    }

    companion object : LoggerHolder()
}
