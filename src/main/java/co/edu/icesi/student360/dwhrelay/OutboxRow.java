package co.edu.icesi.student360.dwhrelay;

import java.util.UUID;

/**
 * One unpublished outbox row: the envelope (payload) plus the attributes a subscriber filters on.
 */
public record OutboxRow(
    UUID id, String eventType, String aggregateType, String aggregateId, String payload) {}
