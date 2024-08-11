package io.github.vooft.kueue

interface KueueConnectionProvider<C, KC : KueueConnection<C>> {
    suspend fun wrap(connection: C): KC
    suspend fun create(): KC
    suspend fun close(connection: KC)
}
