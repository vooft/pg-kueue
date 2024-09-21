rootProject.name = "pg-kueue"

include(
    ":pg-kueue-types",
    ":pg-kueue-utils",
    ":pg-kueue-utils-jooq",

    ":pg-kueue-persistence:pg-kueue-persistence-core",
    ":pg-kueue-persistence:pg-kueue-persistence-schema",
    ":pg-kueue-persistence:pg-kueue-persistence-jdbc",
    ":pg-kueue-persistence:pg-kueue-persistence-jooq",
    ":pg-kueue-persistence:pg-kueue-persistence-jooq-model",

    ":pg-kueue-pubsub:pg-kueue-pubsub-core",
    ":pg-kueue-pubsub:pg-kueue-pubsub-jdbc",
    ":pg-kueue-pubsub:pg-kueue-pubsub-jdbc-test",
    ":pg-kueue-pubsub:pg-kueue-pubsub-jooq",
    ":pg-kueue-pubsub:pg-kueue-pubsub-jooq-test",

    ":pg-kueue-transport:pg-kueue-transport-core",
    ":pg-kueue-transport:pg-kueue-transport-jdbc",

    ":pg-kueue-log:pg-kueue-log-core",
    ":pg-kueue-log:pg-kueue-log-cli:pg-kueue-log-cli-consumer",
    ":pg-kueue-log:pg-kueue-log-cli:pg-kueue-log-cli-migrator",
    ":pg-kueue-log:pg-kueue-log-cli:pg-kueue-log-cli-producer",
    ":pg-kueue-log:pg-kueue-log-jdbc",
    ":pg-kueue-log:pg-kueue-log-test",
)

