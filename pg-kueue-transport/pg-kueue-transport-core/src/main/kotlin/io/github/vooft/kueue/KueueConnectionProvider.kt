package io.github.vooft.kueue

import io.github.vooft.kueue.common.withNonCancellable

interface KueueConnectionProvider<C, KC : KueueConnection<C>> {
    suspend fun wrap(connection: C): KC
    suspend fun create(): KC
    suspend fun close(connection: KC)
}

suspend fun <C, KC : KueueConnection<C>, T> KueueConnectionProvider<C, KC>.withAcquiredConnection(
    existingConnection: KC?,
    block: suspend (KC) -> T
): T {
    val connection = existingConnection ?: create()
    try {
        return block(connection)
    } finally {
        if (existingConnection == null) {
            withNonCancellable { close(connection) }
        }
    }
}
