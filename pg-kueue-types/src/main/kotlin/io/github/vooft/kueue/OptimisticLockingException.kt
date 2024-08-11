package io.github.vooft.kueue

import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class OptimisticLockingException(message: String) : Exception(message)

suspend fun <T> retryingOptimisticLockingException(
    maxRetries: Int = 3,
    timeout: Duration = 200.milliseconds,
    block: suspend () -> T
): T {
    var retries = 0

    @Suppress("detekt:SwallowedException")
    while (retries < maxRetries) {
        try {
            return block()
        } catch (e: OptimisticLockingException) {
            retries++
            delay(timeout)
        }
    }

    return block()
}
