package io.github.vooft.kueue.log.impl.producer

import io.github.vooft.kueue.KueueConnection
import io.github.vooft.kueue.KueueConnectionProvider
import io.github.vooft.kueue.common.withNonCancellable
import io.github.vooft.kueue.common.withVirtualThreadDispatcher
import io.github.vooft.kueue.retryingOptimisticLockingException
import io.github.vooft.kueue.useUnwrapped

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

suspend fun <C, KC : KueueConnection<C>, T> KueueConnectionProvider<C, KC>.withAcquiredConnection(
    existingConnection: KC? = null,
    block: suspend (C) -> T
): T = withConnection(existingConnection) { connection ->
    connection.useUnwrapped { block(it) }
}

suspend fun <C, KC : KueueConnection<C>, T> KueueConnectionProvider<C, KC>.withRetryingAcquiredConnection(
    existingConnection: KC? = null,
    block: suspend (C) -> T
): T = retryingOptimisticLockingException { withAcquiredConnection(existingConnection, block) }
