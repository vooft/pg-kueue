plugins {
    `pg-kueue-base`

    application

    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":pg-kueue-log:pg-kueue-log-core"))

    implementation(project(":pg-kueue-persistence:pg-kueue-persistence-jdbc"))
    implementation(project(":pg-kueue-persistence:pg-kueue-persistence-schema"))

    implementation(libs.clikt)
    implementation(libs.hikaricp)
    implementation(libs.pg.jdbc)
    implementation(libs.slf4j.simple)
}

application {
    mainClass = "io.github.vooft.kueue.cli.PgKueueConsumerCliKt"
}
