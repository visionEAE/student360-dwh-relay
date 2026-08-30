package co.edu.icesi.student360.dwhrelay;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The relay's whole contract, against a real Postgres and the real Pub/Sub wire protocol (the
 * emulator): claims with SKIP LOCKED, publishes the envelope verbatim with routing attributes,
 * marks published_at only after the broker accepted, drains both schemas, and re-running is a
 * no-op.
 */
@Testcontainers
class OutboxRelayServiceIT {

  private static final String PROJECT = "test-project";
  private static final String TOPIC = "student360-events";
  private static final String SUBSCRIPTION = "test-sub";

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

  @Container
  static final PubSubEmulatorContainer PUBSUB =
      new PubSubEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

  private static ManagedChannel channel;
  private static FixedTransportChannelProvider channelProvider;
  private static SubscriptionAdminClient subscriptionAdmin;
  private static Publisher publisher;

  private JdbcTemplate jdbc;
  private OutboxRelayService relay;

  @BeforeAll
  static void createTopicAndSubscription() throws Exception {
    channel = ManagedChannelBuilder.forTarget(PUBSUB.getEmulatorEndpoint()).usePlaintext().build();
    channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

    try (TopicAdminClient topics =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {
      topics.createTopic(TopicName.of(PROJECT, TOPIC));
    }
    subscriptionAdmin =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build());
    subscriptionAdmin.createSubscription(
        ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION).toString(),
        TopicName.of(PROJECT, TOPIC).toString(),
        PushConfig.getDefaultInstance(),
        60);
    publisher =
        Publisher.newBuilder(TopicName.of(PROJECT, TOPIC))
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();
  }

  @AfterAll
  static void shutDown() {
    if (publisher != null) {
      publisher.shutdown();
    }
    if (subscriptionAdmin != null) {
      subscriptionAdmin.close();
    }
    if (channel != null) {
      channel.shutdownNow();
    }
  }

  @BeforeEach
  void setUpDatabaseAndRelay() {
    DriverManagerDataSource dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    jdbc = new JdbcTemplate(dataSource);
    for (String schema : List.of("support", "network")) {
      jdbc.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
      jdbc.execute("DROP TABLE IF EXISTS " + schema + ".outbox_event");
      jdbc.execute(
          "CREATE TABLE "
              + schema
              + ".outbox_event (id UUID PRIMARY KEY, event_type TEXT NOT NULL,"
              + " aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, payload JSONB NOT"
              + " NULL, created_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ)");
    }
    relay =
        new OutboxRelayService(
            jdbc,
            new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
            new PubSubEventSink(publisher),
            Clock.systemUTC(),
            2); // small batch on purpose: the loop must page
    drainSubscription(); // start each test with an empty subscription
  }

  @Test
  void shouldPublishEveryUnpublishedRowMarkItAndPageThroughBatches() {
    for (int i = 0; i < 5; i++) {
      insert("support", "WELLBEING_ENTRY_RECORDED", "S-100" + i);
    }
    insert("network", "SUPPORT_CONNECTION_UPSERTED", "S-2000");

    int supportCount = relay.drain("support");
    int networkCount = relay.drain("network");

    assertThat(supportCount).isEqualTo(5);
    assertThat(networkCount).isEqualTo(1);
    assertThat(unpublished("support")).isZero();
    assertThat(unpublished("network")).isZero();

    List<ReceivedMessage> messages = drainSubscription();
    assertThat(messages).hasSize(6);
    ReceivedMessage first = messages.get(0);
    assertThat(first.getMessage().getData().toStringUtf8()).contains("\"eventId\"");
    assertThat(first.getMessage().getAttributesMap())
        .containsKeys("eventType", "aggregateType", "aggregateId", "sourceSchema");
  }

  @Test
  void shouldBeANoOpWhenEverythingIsAlreadyPublished() {
    insert("support", "ALERT_GENERATED", "S-1003");
    assertThat(relay.drain("support")).isEqualTo(1);
    drainSubscription();

    assertThat(relay.drain("support")).isZero();
    assertThat(drainSubscription()).isEmpty();
  }

  @Test
  void shouldRejectASchemaNameThatIsNotAPlainIdentifier() {
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> relay.drain("support; DROP TABLE x"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void insert(String schema, String eventType, String aggregateId) {
    UUID id = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO "
            + schema
            + ".outbox_event (id, event_type, aggregate_type, aggregate_id, payload, created_at)"
            + " VALUES (?, ?, 'STUDENT', ?, ?::jsonb, now())",
        id,
        eventType,
        aggregateId,
        "{\"eventId\":\"" + id + "\",\"eventType\":\"" + eventType + "\",\"data\":{}}");
  }

  private int unpublished(String schema) {
    Integer count =
        jdbc.queryForObject(
            "SELECT count(*) FROM " + schema + ".outbox_event WHERE published_at IS NULL",
            Integer.class);
    return count == null ? -1 : count;
  }

  private List<ReceivedMessage> drainSubscription() {
    try (var stub =
        com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub.create(
            com.google.cloud.pubsub.v1.stub.SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build())) {
      PullRequest pull =
          PullRequest.newBuilder()
              .setSubscription(ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION).toString())
              .setMaxMessages(100)
              .build();
      List<ReceivedMessage> messages = stub.pullCallable().call(pull).getReceivedMessagesList();
      if (!messages.isEmpty()) {
        stub.acknowledgeCallable()
            .call(
                com.google.pubsub.v1.AcknowledgeRequest.newBuilder()
                    .setSubscription(ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION).toString())
                    .addAllAckIds(messages.stream().map(ReceivedMessage::getAckId).toList())
                    .build());
      }
      return messages;
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }
}
