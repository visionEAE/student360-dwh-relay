package co.edu.icesi.student360.dwhrelay;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Wires the drain; the Pub/Sub adapter binds only when a topic is configured, so tests swap it. */
@Configuration
public class RelayConfiguration {

  @Bean
  public Clock relayClock() {
    return Clock.systemUTC();
  }

  @Bean
  @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
  public EventSink pubSubEventSink(RelayProperties properties) throws IOException {
    // An empty ${PUBSUB_TOPIC:} still counts as "property present" for @ConditionalOnProperty,
    // so the guard lives here: a relay without a destination must say so plainly, not build a
    // Publisher for a nameless topic.
    if (properties.topic() == null || properties.topic().isBlank()) {
      throw new IllegalStateException(
          "PUBSUB_TOPIC is not set — the relay has no destination to publish to");
    }
    Publisher publisher =
        Publisher.newBuilder(TopicName.of(properties.projectId(), properties.topic())).build();
    return new PubSubEventSink(publisher);
  }

  @Bean
  public OutboxRelayService outboxRelayService(
      JdbcTemplate jdbc,
      PlatformTransactionManager transactionManager,
      EventSink sink,
      RelayProperties properties,
      Clock relayClock) {
    return new OutboxRelayService(
        jdbc,
        new TransactionTemplate(transactionManager),
        sink,
        relayClock,
        properties.batchSize());
  }

  @Bean
  public RelayRunner relayRunner(OutboxRelayService relay, RelayProperties properties) {
    return new RelayRunner(relay, properties);
  }
}
