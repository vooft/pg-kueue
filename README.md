![Build and test](https://github.com/vooft/pg-kueue/actions/workflows/build.yml/badge.svg?branch=main)
![Releases](https://img.shields.io/github/v/release/vooft/pg-kueue)
![Maven Central](https://img.shields.io/maven-central/v/io.github.vooft/pg-kueue-core)
![License](https://img.shields.io/github/license/vooft/pg-kueue)

# pg-kueue
This project aims to provide a simple Kotlin Coroutines-based interface for messaging, built on top of PostgreSQL, consists of two sub-projects:
1. `pg-kueue-log` -- a library for emulating event store using PostgreSQL, similar to the Kafka Producer/Consumer model.
2. `pg-kueue-pubsub` -- a simple library for working with PostgreSQL LISTEN/NOTIFY.

# pg-kueue-log
This module provides 2 main classes: `KueueProducer` and `KueueConsumer`, which mirror the Kafka Producer/Consumer model.

At the moment it only works on top of the JDBC, however it should be possible to add other SPIs in future.

## Usage
```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.vooft:pg-kueue-log-jdbc:<version>")
}
```

Simple consumer usage:
```kotlin
// dataSource must be provided externally
val dataSource = createMyDataSource()
val kueueLog = KueueLog.jdbc(dataSource)

// here consumer will try to subscribe to the topic and group, if it does not exist, it will be created
// topic must exist before creating a consumer
val consumer = kueueLog.createConsumer(KueueTopic("my-topic"), KueueConsumerGroup("my-group"))

// messages are an instance of ReceiveChannel and could be consumed using a for loop 
for (message in consumer.messages) {
    println(message)
}
```

Simple producer usage:
```kotlin
// dataSource must be provided externally
val dataSource = createMyDataSource()
val kueueLog = KueueLog.jdbc(dataSource)

// topic must exist before creating a producer
val producer = kueueLog.createProducer(KueueTopic("test"))

// key is mandatory and is used to partition messages
producer.produce(KueueKey("my-key"), KueueValue("my-value"))
```

# pg-kueue-pubsub
Kotlin Coroutines PostgresSQL-based message queue using LISTEN/NOTIFY

Everything is String-based and for now just follows normal LISTEN/NOTIFY rules.

## JDBC usage

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.vooft:pg-kueue-jdbc:<version>")
}
```

Simple usage:
```kotlin
val dataSource = createMyDataSource()

val kueue = KueuePubSub.jdbc(dataSource)
val subscription = kueue.subscribe(KueueTopic("my_topic")) { message: String ->
    println("Received message: $message")
}

kueue.publish(KueueTopic("my_topic"), "Hello, world!")
// will print after a tiny delay: "Received message: Hello, world!"
```

You can close subscription, if you would like to stop a particular listener:
```kotlin
subscription.close()
```

But it is not necessary if the the subscription should exist for the whole Kueue lifecycle.

All subscriptions will be closed automatically when Kueue is closed:
```kotlin
kueue.close()
```

## Transactional usage
To publish a message using existing transaction, you should provide the transactional connection.

Normally, API accepts a instance of a wrapped connection `KueueConnection`, there is a helper method to create it:
```kotlin
val transactionalConnection = myBeginTransaction()
kueue.publish(KueueTopic("my_topic"), "Hello, world!", kueue.wrap(transactionalConnection))
``` 

There is also an extension function for a specific library to simplify transactional publishing:
```kotlin
val transactionalConnection = myBeginTransaction()
kueue.publish(KueueTopic("my_topic"), "Hello, world!", transactionalConnection) // an extension function must be imported explicitly
```
