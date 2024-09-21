package io.github.vooft.kueue.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.jdbc.JdbcDataSourceKueueConnectionProvider
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerDao
import io.github.vooft.kueue.log.impl.consumer.KueueConsumerImpl
import io.github.vooft.kueue.log.impl.poller.PersisterKueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.KueueConsumerGroup
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import kotlinx.coroutines.runBlocking
import java.util.UUID
import javax.sql.DataSource

class PgKueueConsumerCli : CliktCommand() {
    val jdbcUrl: String by option().required().help("JDBC URL to connect to the database")
    val username: String by option().required().help("Database username to connect to the database")
    val password: String by option().required().help("Database password to connect to the database")

    val topic: String by option().required().help("Topic to consume messages from")
    val group: String? by option().help("Consumer group name")

    override fun run() {
        HikariDataSource(
            HikariConfig().also {
                it.jdbcUrl = jdbcUrl
                it.username = username
                it.password = password
            }
        ).use {
            runBlocking {
                runConsumer(it)
            }
        }
    }

    private suspend fun runConsumer(dataSource: DataSource) {
        val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
        val consumer = KueueConsumerImpl(
            topic = KueueTopic(topic),
            consumerGroup = KueueConsumerGroup(group ?: UUID.randomUUID().toString()),
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

        for (message in consumer.messages) {
            println("$message")
        }
    }
}

fun main(args: Array<String>) = PgKueueConsumerCli().main(args)
