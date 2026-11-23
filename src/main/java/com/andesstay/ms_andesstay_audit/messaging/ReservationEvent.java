package com.andesstay.ms_andesstay_audit.messaging;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

/**
 * Contrato consumido desde el topico reservations.events, publicado por
 * ms-andesstay-reservations en cada cambio de estado de una reserva (creacion,
 * confirmacion, check-in, check-out, cancelacion). El envelope
 * type/eventId/timestamp/traceId/correlationId es el mismo acordado para las colas de
 * RabbitMQ de notify.
 */
public record ReservationEvent(
        @NotBlank String type,
        @NotNull String eventId,
        @NotNull Instant timestamp,
        @NotBlank String traceId,
        @NotBlank String correlationId,
        @NotBlank String reservationId,
        String previousStatus,
        @NotBlank String newStatus,
        String actorId,
        String actorRole,
        @NotNull Long unitId
) {
}
