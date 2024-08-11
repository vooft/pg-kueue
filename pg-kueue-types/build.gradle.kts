plugins {
    `pg-kueue-base`
    `pg-kueue-publish`
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
}
