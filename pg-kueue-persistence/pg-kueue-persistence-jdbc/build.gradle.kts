plugins {
    `pg-kueue-base`
    `pg-kueue-publish`
}

dependencies {
    api(project(":pg-kueue-persistence:pg-kueue-persistence-core"))
    api(project(":pg-kueue-transport:pg-kueue-transport-jdbc"))
    api(project(":pg-kueue-persistence:pg-kueue-persistence-jooq-model"))

    implementation(project(":pg-kueue-utils"))

    compileOnly(libs.pg.jdbc)

    testImplementation(testFixtures(project(":pg-kueue-utils")))
    testImplementation(project(":pg-kueue-persistence:pg-kueue-persistence-schema"))
    testImplementation(libs.bundles.junit)
    testImplementation(libs.bundles.kotest)
    testImplementation(libs.pg.jdbc)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.bundles.flyway)
}
