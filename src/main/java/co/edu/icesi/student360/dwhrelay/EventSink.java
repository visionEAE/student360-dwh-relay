package co.edu.icesi.student360.dwhrelay;

/**
 * Where drained rows go. A port so the relay's transactional loop can be tested against the Pub/Sub
 * emulator — or any fake — without touching the loop itself.
 */
public interface EventSink {

  /** Publishes one envelope; returns only after the sink has durably accepted it. */
  void publish(String sourceSchema, OutboxRow row);
}
