plugins {
    `pg-kueue-base`
}

dependencies {
    testImplementation(project(":pg-kueue-log:pg-kueue-log-core"))
    testImplementation(project(":pg-kueue-persistence:pg-kueue-persistence-jdbc"))
    testImplementation(project(":pg-kueue-persistence:pg-kueue-persistence-schema"))

    testImplementation(libs.bundles.test)
    testImplementation(testFixtures(project(":pg-kueue-utils")))
}
