package io.github.vooft.kueue.log.impl

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.common.withNonCancellable
import io.github.vooft.kueue.common.withVirtualThreadDispatcher

suspend fun <C, KC : KueueConnection<C>, T> KueueConnectionProvider<C, KC>.withConnection(
    existingConnection: KC? = null,
    block: suspend (KC) -> T
): T = withVirtualThreadDispatcher {
    val acquiredConnection = existingConnection ?: create()
    try {
        return@withVirtualThreadDispatcher block(acquiredConnection)
    } finally {
        if (existingConnection == null) {
            withNonCancellable { close(acquiredConnection) }
        }
    }
}
