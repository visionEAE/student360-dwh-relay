package co.edu.icesi.student360.dwhrelay;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The whole application context boots — the test the first cloud run showed was missing: the
 * unit-level IT proved the drain logic while the context itself could not start (Jackson absent for
 * common's audit wiring). The sink is stubbed; the runner executes against empty outbox tables and
 * the app exits cleanly.
 */
@SpringBootTest(
    properties = {
      "DWH_RELAY_DB_PASSWORD=unused-overridden-by-testcontainers",
      "relay.topic=test-topic",
      "relay.project-id=test-project"
    })
@Testcontainers
@Import(DwhRelayApplicationIT.StubSink.class)
class DwhRelayApplicationIT {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

  @TestConfiguration
  static class StubSink {
    @Bean
    EventSink stubEventSink() {
      return (schema, row) -> {};
    }
  }

  @Autowired private JdbcTemplate jdbc;
  @Autowired private OutboxRelayService relay;

  @Test
  void shouldBootTheFullContextAndDrainAnEmptySchemaCleanly() {
    jdbc.execute("CREATE SCHEMA IF NOT EXISTS support");
    jdbc.execute(
        "CREATE TABLE IF NOT EXISTS support.outbox_event (id UUID PRIMARY KEY, event_type TEXT"
            + " NOT NULL, aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, payload JSONB"
            + " NOT NULL, created_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ)");
    assertThat(relay.drain("support")).isZero();
  }
}
