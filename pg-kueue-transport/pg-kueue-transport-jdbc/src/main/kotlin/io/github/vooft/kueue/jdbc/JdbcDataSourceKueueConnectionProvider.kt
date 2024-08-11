package io.github.vooft.kueue.jdbc

import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.common.withVirtualThreadDispatcher
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

class JdbcDataSourceKueueConnectionProvider(private val dataSource: DataSource) : KueueConnectionProvider<Connection, JdbcKueueConnection> {

    private val acquired = AtomicInteger()

    override suspend fun wrap(connection: Connection) = JdbcKueueConnection(connection = connection)

    override suspend fun create(): JdbcKueueConnection = withVirtualThreadDispatcher {
        println("create: acquired ${acquired.get()}")
        JdbcKueueConnection(connection = dataSource.connection)
    }.also { acquired.incrementAndGet() }

    override suspend fun close(connection: JdbcKueueConnection) = withVirtualThreadDispatcher {
        println("close: acquired ${acquired.get()}")
        connection.connection.close()
    }.also { acquired.decrementAndGet() }
}
