plugins {
    `pg-kueue-base`
    `pg-kueue-publish`
}

dependencies {
    api(project(":pg-kueue-log:pg-kueue-log-core"))
    api(project(":pg-kueue-persistence:pg-kueue-persistence-jdbc"))
    api(libs.bundles.coroutines)
}
