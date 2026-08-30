package co.edu.icesi.student360.dwhrelay;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The transactional drain. Per batch, inside ONE transaction: claim unpublished rows with {@code
 * FOR UPDATE SKIP LOCKED} — overlapping scheduler runs simply skip each other's claims instead of
 * blocking or double-publishing — publish each to the sink, and only then mark {@code
 * published_at}. A crash between a publish and the commit re-delivers those rows next run:
 * at-least-once by design, deduplicated downstream by the envelope's eventId.
 *
 * <p>Schema names are validated against a strict identifier pattern because they are interpolated
 * into SQL — they come from configuration, not user input, but the check costs nothing.
 */
public class OutboxRelayService {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);
  private static final String SCHEMA_PATTERN = "[a-z_][a-z0-9_]*";

  private final JdbcTemplate jdbc;
  private final TransactionTemplate transaction;
  private final EventSink sink;
  private final Clock clock;
  private final int batchSize;

  public OutboxRelayService(
      JdbcTemplate jdbc,
      TransactionTemplate transaction,
      EventSink sink,
      Clock clock,
      int batchSize) {
    this.jdbc = jdbc;
    this.transaction = transaction;
    this.sink = sink;
    this.clock = clock;
    this.batchSize = batchSize;
  }

  /** Drains one schema's outbox completely; returns how many rows were published. */
  public int drain(String schema) {
    if (!schema.matches(SCHEMA_PATTERN)) {
      throw new IllegalArgumentException("Invalid schema name: " + schema);
    }
    int total = 0;
    int published;
    do {
      published = transaction.execute(status -> drainBatch(schema));
      total += published;
    } while (published == batchSize); // a short batch means the table is (currently) empty
    if (total > 0) {
      log.info("Relayed {} events from {}.outbox_event", total, schema);
    }
    return total;
  }

  private int drainBatch(String schema) {
    List<OutboxRow> rows =
        jdbc.query(
            "SELECT id, event_type, aggregate_type, aggregate_id, payload::text"
                + " FROM "
                + schema
                + ".outbox_event"
                + " WHERE published_at IS NULL ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED",
            (rs, i) ->
                new OutboxRow(
                    rs.getObject("id", UUID.class),
                    rs.getString("event_type"),
                    rs.getString("aggregate_type"),
                    rs.getString("aggregate_id"),
                    rs.getString("payload")),
            batchSize);
    for (OutboxRow row : rows) {
      sink.publish(schema, row);
      jdbc.update(
          "UPDATE " + schema + ".outbox_event SET published_at = ? WHERE id = ?",
          Timestamp.from(clock.instant()),
          row.id());
    }
    return rows.size();
  }
}
