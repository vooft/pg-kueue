plugins {
    `pg-kueue-base`
    `pg-kueue-publish`
}

dependencies {
    api(project(":pg-kueue-types"))
    api(project(":pg-kueue-transport:pg-kueue-transport-core"))
    api(project(":pg-kueue-persistence:pg-kueue-persistence-core"))

    implementation(project(":pg-kueue-utils"))

    api(libs.bundles.coroutines)
}
