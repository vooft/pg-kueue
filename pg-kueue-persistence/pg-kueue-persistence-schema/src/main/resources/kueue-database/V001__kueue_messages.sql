CREATE TABLE topics (
    name TEXT PRIMARY KEY,
    partitions INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_index INT NOT NULL,
    partition_offset INT NOT NULL,
    key TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (topic) REFERENCES topics (name)
);

CREATE INDEX messages_topic_idx ON messages (topic);
CREATE UNIQUE INDEX messages_topic_partition_index ON messages (topic, partition_index, partition_offset);

CREATE TABLE topic_partitions (
    topic TEXT NOT NULL,
    partition_index INT NOT NULL,
    next_partition_offset INT NOT NULL,
    version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (topic, partition_index),
    FOREIGN KEY (topic) REFERENCES topics (name)
);

CREATE INDEX topic_partitions_topic_idx ON topic_partitions (topic);




