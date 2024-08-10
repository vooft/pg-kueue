CREATE TABLE topic (
    name TEXT PRIMARY KEY,
    partitions INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    partition_offset INT NOT NULL,
    key TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (topic) REFERENCES topic (name)
);

CREATE INDEX messages_topic_partition_index ON messages (topic, partition, partition_offset);

CREATE TABLE topic_partitions (
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    next_partition_offset INT NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (topic, partition),
    FOREIGN KEY (topic) REFERENCES topic (name)
);

CREATE TABLE consumer_groups (
    name TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE committed_offsets (
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INT NOT NULL,
    partition_offset INT NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (group_name, topic, partition),
    FOREIGN KEY (topic) REFERENCES topic (name),
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name),
    FOREIGN KEY (topic, partition) REFERENCES topic_partitions (topic, partition)
);

CREATE INDEX committed_offsets_group_name_idx ON committed_offsets (group_name);
CREATE INDEX committed_offsets_topic_partition_idx ON committed_offsets (topic, partition);

CREATE TABLE connected_consumers (
    id UUID PRIMARY KEY,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    assigned_partitions INT[] NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name),
    FOREIGN KEY (topic) REFERENCES topic (name)
);

CREATE INDEX connected_consumers_group_name_idx ON connected_consumers (group_name);
CREATE INDEX connected_consumers_last_heartbeat_idx ON connected_consumers (last_heartbeat);

CREATE TABLE consumer_group_leader_locks (
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    consumer_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,

    PRIMARY KEY (group_name, topic),
    FOREIGN KEY (topic) REFERENCES topic (name),
    FOREIGN KEY (group_name) REFERENCES consumer_groups (name),
    FOREIGN KEY (consumer_id) REFERENCES connected_consumers (id)
);

CREATE INDEX consumer_group_leader_locks_group_name_idx ON consumer_group_leader_locks (group_name);
CREATE INDEX consumer_group_leader_locks_group_name_topic_idx ON consumer_group_leader_locks (group_name, topic);
CREATE INDEX consumer_group_leader_locks_consumer_id_idx ON consumer_group_leader_locks (consumer_id);



