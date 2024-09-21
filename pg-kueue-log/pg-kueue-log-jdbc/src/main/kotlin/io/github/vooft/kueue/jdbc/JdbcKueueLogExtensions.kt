package io.github.vooft.kueue.jdbc

import io.github.vooft.kueue.log.KueueLog
import io.github.vooft.kueue.log.impl.KueueLogImpl
import io.github.vooft.kueue.log.impl.poller.PersisterKueueConsumerMessagePoller
import io.github.vooft.kueue.persistence.jdbc.JdbcKueuePersister
import java.sql.Connection
import javax.sql.DataSource

fun KueueLog.Companion.jdbc(dataSource: DataSource): KueueLog<Connection, JdbcKueueConnection> {
    val connectionProvider = JdbcDataSourceKueueConnectionProvider(dataSource)
    return KueueLogImpl(
        connectionProvider = connectionProvider,
        persister = JdbcKueuePersister(),
        poller = PersisterKueueConsumerMessagePoller(connectionProvider, JdbcKueuePersister())
    )
}
