package io.github.vooft.kueue.log.impl

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.common.withNonCancellable

suspend fun <C, KC : KueueConnection<C>, T> KueueConnectionProvider<C, KC>.withConnection(
    existingConnection: KC? = null,
    block: suspend (KC) -> T
): T {
    val acquiredConnection = existingConnection ?: create()
    try {
        return block(acquiredConnection)
    } finally {
        if (existingConnection == null) {
            withNonCancellable { close(acquiredConnection) }
        }
    }
}
