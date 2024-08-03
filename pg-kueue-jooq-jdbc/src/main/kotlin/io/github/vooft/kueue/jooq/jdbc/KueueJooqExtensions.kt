package io.github.vooft.kueue.jooq.jdbc

import io.github.vooft.kueue.Kueue
import io.github.vooft.kueue.KueueTopic
import io.github.vooft.kueue.impl.KueueImpl
import io.github.vooft.kueue.jdbc.DataSourceKueueConnectionProvider
import io.github.vooft.kueue.jdbc.JdbcKueueConnection
import io.github.vooft.kueue.jdbc.JdbcKueueConnectionPubSub
import org.jooq.DSLContext
import java.sql.Connection
import javax.sql.DataSource

typealias JoodJdbcKueueConnection = JdbcKueueConnection
typealias JooqDataSourceKueueConnectionProvider = DataSourceKueueConnectionProvider
typealias JooqJdbcKueueConnectionPubSub = JdbcKueueConnectionPubSub

fun Kueue.Companion.jooq(dataSource: DataSource): Kueue<Connection, JoodJdbcKueueConnection> = KueueImpl(
    connectionProvider = JooqDataSourceKueueConnectionProvider(dataSource),
    pubSub = JooqJdbcKueueConnectionPubSub()
)

suspend fun Kueue<Connection, JoodJdbcKueueConnection>.send(topic: KueueTopic, message: String, transactionalDsl: DSLContext) {
    val connectionProvider = transactionalDsl.configuration().connectionProvider()
    val connection = requireNotNull(connectionProvider.acquire()) { "Unable to acquire connection from DSLContext" }

    try {
        send(topic, message, wrap(connection))
    } finally {
        connectionProvider.release(connection)
    }
}