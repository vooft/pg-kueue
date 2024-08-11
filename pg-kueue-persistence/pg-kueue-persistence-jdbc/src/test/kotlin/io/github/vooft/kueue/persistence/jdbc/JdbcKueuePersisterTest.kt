package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.IntegrationTest
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.BeforeEach

class JdbcKueuePersisterTest : IntegrationTest() {

    private val persister = JdbcKueuePersister()

    @BeforeEach
    fun setUp() {
        Flyway.configure()
            .dataSource(psql.jdbcUrl, psql.username, psql.password)
            .locations("classpath:kueue-database")
            .load()
            .migrate()

        psql.createConnection("").use {
            it.createStatement().execute("TRUNCATE kueue_events CASCADE")
        }
    }

}
