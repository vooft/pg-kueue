package io.github.vooft.kueue

interface KueueConnection<C> {
    val isClosed: Boolean

    suspend fun acquire(): C
    suspend fun release()
}

suspend fun <C, T> KueueConnection<C>.useUnwrapped(block: suspend (C) -> T): T {
    val connection = acquire()
    try {
        return block(connection)
    } finally {
        release()
    }
}
