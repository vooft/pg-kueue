package io.github.vooft.kueue.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import org.flywaydb.core.Flyway

class PgKueueMigratorCli : CliktCommand() {

    val jdbcUrl: String by option().required().help("JDBC URL to connect to the database")
    val username: String by option().required().help("Database username to connect to the database")
    val password: String by option().required().help("Database password to connect to the database")

    override fun run() {
        Flyway.configure()
            .dataSource(jdbcUrl, username, password)
            .locations("classpath:kueue-database")
            .load()
            .migrate()

    }
}

fun main(args: Array<String>) = PgKueueMigratorCli().main(args)
