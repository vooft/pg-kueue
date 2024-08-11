package io.github.vooft.kueue.persistence

import java.time.Instant
import java.time.temporal.ChronoUnit

internal fun now() = Instant.now().truncatedTo(ChronoUnit.MICROS)
