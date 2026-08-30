package co.edu.icesi.student360.dwhrelay;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "relay")
public record RelayProperties(
    /** Pub/Sub topic the envelopes are published to. */
    String topic,
    /** GCP project the topic lives in. */
    String projectId,
    /** Schemas whose outbox_event tables are drained. */
    @DefaultValue({"support", "network"}) List<String> schemas,
    /** Rows claimed per transaction. */
    @DefaultValue("100") int batchSize) {}
