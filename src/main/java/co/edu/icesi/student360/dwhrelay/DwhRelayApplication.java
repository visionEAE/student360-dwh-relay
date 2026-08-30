package co.edu.icesi.student360.dwhrelay;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The data-warehouse relay, packaged as a Cloud Run job: drains the services' outbox tables into
 * Pub/Sub and exits. Runs on a schedule (Cloud Scheduler, terraform-core) or by hand ({@code gcloud
 * run jobs execute s360-relay}).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class DwhRelayApplication {

  public static void main(String[] args) {
    Dotenv.configure().ignoreIfMissing().load().entries().stream()
        .filter(entry -> System.getenv(entry.getKey()) == null)
        .filter(entry -> System.getProperty(entry.getKey()) == null)
        .forEach(entry -> System.setProperty(entry.getKey(), entry.getValue()));
    System.exit(SpringApplication.exit(SpringApplication.run(DwhRelayApplication.class, args)));
  }
}
