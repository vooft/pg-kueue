CREATE TABLE consumer_groups
(
    name TEXT PRIMARY KEY,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE consumer_group_leader_locks
(
    group_name     TEXT                     NOT NULL,
    topic          TEXT                     NOT NULL,
    consumer_name  TEXT                     NOT NULL,
    version        INT                      NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,

    PRIMARY KEY (group_name, topic),
    FOREIGN KEY (topic) REFERENCES topics (name),
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name) -- covered by primary key
);

CREATE INDEX consumer_group_leader_locks_topic_idx ON consumer_group_leader_locks (topic);

CREATE TABLE connected_consumers
(
    consumer_name       TEXT,
    group_name          TEXT                     NOT NULL,
    topic               TEXT                     NOT NULL,
    assigned_partitions INT[]                    NOT NULL,
    status              TEXT                     NOT NULL,
    version             INT                      NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat      TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (group_name, topic, consumer_name),
    FOREIGN KEY (topic) REFERENCES topics (name),
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name) -- covered by primary key
);

CREATE INDEX connected_consumers_topic_idx ON topics (name);
CREATE INDEX connected_consumers_last_heartbeat_idx ON connected_consumers (last_heartbeat);

CREATE TABLE committed_offsets
(
    group_name       TEXT                     NOT NULL,
    topic            TEXT                     NOT NULL,
    partition_index  INT                      NOT NULL,
    partition_offset INT                      NOT NULL,
    version          INT                      NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (group_name, topic, partition_index),
    FOREIGN KEY (topic) REFERENCES topics (name),
    FOREIGN KEY (topic, partition_index) REFERENCES topic_partitions (topic, partition_index),
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name) -- covered by primary key
);

CREATE INDEX committed_offsets_topic_partition_idx ON committed_offsets (topic, partition_index);
