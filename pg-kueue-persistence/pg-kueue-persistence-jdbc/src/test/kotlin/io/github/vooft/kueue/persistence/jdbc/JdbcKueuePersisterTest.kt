package io.github.vooft.kueue.persistence.jdbc

import io.github.vooft.kueue.IntegrationTest
import io.github.vooft.kueue.jdbc.JdbcKueueConnection
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
            it.createStatement().execute("TRUNCATE topics CASCADE")
        }
    }

    @Test
    fun `foo bar`(): Unit = runBlocking {
        psql.createConnection("").use { jdbcConnection ->
            persister.withTransaction(JdbcKueueConnection(jdbcConnection)) { connection ->
                println("foo bar")
            }
        }
    }
}
