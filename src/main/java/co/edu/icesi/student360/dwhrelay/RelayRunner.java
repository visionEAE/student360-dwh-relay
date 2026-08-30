package co.edu.icesi.student360.dwhrelay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

/** The Cloud Run job body: drain every configured schema, log the totals, exit. */
public class RelayRunner implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(RelayRunner.class);

  private final OutboxRelayService relay;
  private final RelayProperties properties;

  public RelayRunner(OutboxRelayService relay, RelayProperties properties) {
    this.relay = relay;
    this.properties = properties;
  }

  @Override
  public void run(String... args) {
    int total = 0;
    for (String schema : properties.schemas()) {
      total += relay.drain(schema);
    }
    log.info("Relay run complete: {} events published to {}", total, properties.topic());
  }
}
