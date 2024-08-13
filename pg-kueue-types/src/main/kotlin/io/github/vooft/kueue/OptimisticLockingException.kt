package io.github.vooft.kueue

import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.nanoseconds

class OptimisticLockingException(message: String) : Exception(message)

suspend fun <T> retryingOptimisticLockingException(
    maxRetries: Int = 5,
    timeoutFrom: Duration = 50.milliseconds,
    timeoutTo: Duration = 100.milliseconds,
    block: suspend () -> T
): T {
    var retries = 0

    @Suppress("detekt:SwallowedException")
    while (retries < maxRetries) {
        try {
            return block()
        } catch (e: OptimisticLockingException) {
            retries++
            delay(Random.nextLong(timeoutFrom.inWholeNanoseconds, timeoutTo.inWholeNanoseconds).nanoseconds)
        }
    }

    return block()
}

suspend fun swallowOptimisticLockingException(block: suspend () -> Unit) {
    try {
        block()
    } catch (_: OptimisticLockingException) {
        // ignore
    }
}
