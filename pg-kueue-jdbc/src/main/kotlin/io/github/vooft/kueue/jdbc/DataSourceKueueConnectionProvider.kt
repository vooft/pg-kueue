package io.github.vooft.kueue.jdbc

import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.common.withVirtualThreadDispatcher
import java.sql.Connection
import javax.sql.DataSource

class DataSourceKueueConnectionProvider(private val dataSource: DataSource) : KueueConnectionProvider<Connection, JdbcKueueConnection> {

    override suspend fun wrap(connection: Connection) = JdbcKueueConnection(jdbcConnection = connection)

    override suspend fun create(): JdbcKueueConnection = withVirtualThreadDispatcher {
        JdbcKueueConnection(jdbcConnection = dataSource.connection)
    }

    override suspend fun close(connection: JdbcKueueConnection) = withVirtualThreadDispatcher {
        connection.jdbcConnection.close()
    }
}
