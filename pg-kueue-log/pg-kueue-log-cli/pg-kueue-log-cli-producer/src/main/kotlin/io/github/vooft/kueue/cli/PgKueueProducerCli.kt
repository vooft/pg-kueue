package io.github.vooft.kueue.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt
import com.github.ajalt.clikt.parameters.options.required
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.jdbc.JdbcDataSourceKueueConnectionProvider
import io.github.vooft.kueue.log.impl.producer.KueueProducerImpl
import io.github.vooft.kueue.persistence.KueueKey
import io.github.vooft.kueue.persistence.KueueValue
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import kotlinx.coroutines.runBlocking
import java.util.UUID
import javax.sql.DataSource

class PgKueueProducerCli : CliktCommand() {
    val jdbcUrl: String by option().required().help("JDBC URL to connect to the database")
    val username: String by option().required().help("Database username to connect to the database")
    val password: String by option().required().help("Database password to connect to the database")

    val topic: String by option().required().help("Topic to consume messages from")
    val message: String by option().prompt("Message").help("Message to send")

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
        val producer = KueueProducerImpl(
            topic = KueueTopic(topic),
            connectionProvider = connectionProvider,
            persister = JdbcKueuePersister()
        )

        producer.produce(KueueKey(UUID.randomUUID().toString()), KueueValue(message))
    }
}

fun main(args: Array<String>) = PgKueueProducerCli().main(args)
