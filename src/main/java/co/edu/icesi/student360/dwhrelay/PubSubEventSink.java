package co.edu.icesi.student360.dwhrelay;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Publishes the stored envelope verbatim — it was written as the exact message a subscriber should
 * receive — with routing attributes alongside. publish() blocks on the broker's ack: the relay's
 * contract is that a row is only marked published after the sink durably accepted it.
 *
 * <p>No ordering keys: the BigQuery subscription writes unordered regardless, the envelope carries
 * its own timestamps for query-time ordering, and an ordering key would cap throughput for nothing.
 */
public class PubSubEventSink implements EventSink {

  private final com.google.cloud.pubsub.v1.Publisher publisher;

  public PubSubEventSink(com.google.cloud.pubsub.v1.Publisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public void publish(String sourceSchema, OutboxRow row) {
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(row.payload(), StandardCharsets.UTF_8))
            .putAllAttributes(
                Map.of(
                    "eventType", row.eventType(),
                    "aggregateType", row.aggregateType(),
                    "aggregateId", row.aggregateId(),
                    "sourceSchema", sourceSchema))
            .build();
    try {
      publisher.publish(message).get();
    } catch (ExecutionException exception) {
      throw new IllegalStateException("Pub/Sub rejected event " + row.id(), exception.getCause());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted publishing event " + row.id(), exception);
    }
  }
}
