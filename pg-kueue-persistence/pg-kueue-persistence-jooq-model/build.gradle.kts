import org.jooq.codegen.KotlinGenerator
import org.jooq.meta.jaxb.ForcedType

plugins {
    `pg-kueue-base`
    `pg-kueue-publish`
    alias(libs.plugins.jooq.docker)
}

dependencies {
    jooqCodegen(libs.pg.jdbc)
    api(libs.jooq)
}

val schemeProject = project(":pg-kueue-persistence:pg-kueue-persistence-schema")

tasks {
    generateJooqClasses {
        migrationLocations.setFromFilesystem(schemeProject.files("src/main/resources/kueue-database"))
        basePackageName.set("io.github.vooft.kueue.generated.sql")
        usingJavaConfig {
            this.name = KotlinGenerator::class.qualifiedName
            this.generate.isKotlinNotNullRecordAttributes = true
            this.database.withForcedTypes(
                ForcedType().withName("INSTANT").withIncludeTypes("(?i:TIMESTAMP\\ WITH\\ TIME\\ ZONE)"),
            )
        }
    }
}
