CREATE TABLE consumer_groups
(
    name       TEXT                     NOT NULL,
    topic      TEXT                     NOT NULL,
    status     TEXT                     NOT NULL,
    version    INT                      NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    PRIMARY KEY (name, topic),
    FOREIGN KEY (topic) REFERENCES topics (name)
);

CREATE INDEX consumer_groups_topic_idx ON consumer_groups (topic);

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
    FOREIGN KEY (group_name, topic) REFERENCES consumer_groups (name, topic)
);

CREATE INDEX consumer_group_leader_locks_group_name_idx ON consumer_group_leader_locks (group_name);
CREATE INDEX consumer_group_leader_locks_group_name_topic_idx ON consumer_group_leader_locks (group_name, topic);
CREATE INDEX consumer_group_leader_locks_consumer_name_idx ON consumer_group_leader_locks (consumer_name);

CREATE TABLE connected_consumers
(
    consumer_name       TEXT,
    group_name          TEXT                     NOT NULL,
    topic               TEXT                     NOT NULL,
    assigned_partitions INT[]                    NOT NULL,
    version             INT                      NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat      TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (consumer_name, group_name, topic),
    FOREIGN KEY (group_name, topic) REFERENCES consumer_groups (name, topic),
    FOREIGN KEY (topic) REFERENCES topics (name)
);

CREATE INDEX connected_consumers_group_name_idx ON connected_consumers (group_name);
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
    FOREIGN KEY (group_name, topic) REFERENCES consumer_groups (name, topic),
    FOREIGN KEY (topic, partition_index) REFERENCES topic_partitions (topic, partition_index)
);

CREATE INDEX committed_offsets_group_name_idx ON committed_offsets (group_name);
CREATE INDEX committed_offsets_topic_partition_idx ON committed_offsets (topic, partition_index);
