package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import lab.support.Lab;
import org.junit.jupiter.api.Test;

/** One end-to-end flow through a real broker and a real database. */
class SmokeTest {

    @Test
    void plus100ThenMinus30GivesBalance70() {
        try (var lab = new Lab()) {
            lab.startConsumer();
            lab.producer.publish(lab.topic, List.of(pay("acc-1", 1, 100), pay("acc-1", 2, -30)));

            // Wait until every produced message is committed by the ledger group (lag 0); no fixed sleeps.
            await().atMost(Duration.ofSeconds(20)).until(() -> lab.lag() == 0);
            assertThat(lab.ledger.balanceOf("acc-1")).isEqualTo(70);
        }
    }
}
