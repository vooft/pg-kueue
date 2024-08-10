package io.github.vooft.kueue

interface KueueConnection<C, KC: KueueConnection<C, KC>> {
    val isClosed: Boolean

    suspend fun acquire(): C
    suspend fun release()

    suspend fun <T> withTransaction(block: suspend (KC) -> T): T
}

suspend fun <C, KC: KueueConnection<C, KC>, T> KueueConnection<C, KC>.useUnwrapped(block: suspend (C) -> T): T {
    val connection = acquire()
    try {
        return block(connection)
    } finally {
        release()
    }
}
