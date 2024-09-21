plugins {
    `pg-kueue-base`
    `pg-kueue-publish`

    application

    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":pg-kueue-persistence:pg-kueue-persistence-schema"))

    implementation(libs.clikt)
    implementation(libs.hikaricp)
    implementation(libs.flyway.core)
    implementation(libs.flyway.postgres)
    implementation(libs.pg.jdbc)
}

application {
    mainClass = "io.github.vooft.kueue.cli.PgKueueMigratorCliKt"
}
