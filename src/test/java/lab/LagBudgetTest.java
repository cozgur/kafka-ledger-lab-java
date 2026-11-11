package lab;

import static lab.support.Payments.pay;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lab.payments.PaymentCaptured;
import lab.support.Lab;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * After a burst, consumer lag (admin API) drains within a time budget. The budget is a product decision; on a single
 * broker it is a regression guard, not proof of a production SLO. Runs ALONE (`./gradlew perfTest`), without
 * competing load: in the TS lab, running it next to 11 parallel test files made the drain time spread from 3.4 to 6.2s.
 */
@Tag("perf")
class LagBudgetTest {

    static final long BUDGET_MS = Long.parseLong(System.getProperty("lagBudgetMs", "5000"));

    @Test
    void aBurstOf5000PaymentsDrainsToLag0WithinTheBudget() throws Exception {
        try (var lab = new Lab(3)) {
            lab.startConsumer();
            List<PaymentCaptured> events = new ArrayList<>();
            for (int i = 1; i <= 100; i++) for (int a = 0; a < 50; a++) events.add(pay("acc-" + a, i, 1));

            long start = System.currentTimeMillis();
            lab.producer.publish(lab.topic, events);
            long publishedMs = System.currentTimeMillis() - start;

            // Sample lag (broker, committed offsets) and the DB's applied count together every 100ms.
            List<String> samples = new ArrayList<>();
            long lag, applied, t;
            do {
                lag = lab.lag();
                applied = lab.ledger.appliedCount();
                t = System.currentTimeMillis() - start;
                samples.add("{\"t\":" + t + ",\"lag\":" + lag + ",\"applied\":" + applied + "}");
                if (lag == 0) break;
                Thread.sleep(100); // sampling interval, not a wait-for-result sleep
            } while (t < BUDGET_MS * 4);

            long drainMs = t;
            String result = String.format("{\"events\":%d,\"publishedMs\":%d,\"drainMs\":%d,\"budgetMs\":%d,\"eventsPerSec\":%d,\"samples\":[%s]}",
                    events.size(), publishedMs, drainMs, BUDGET_MS, events.size() * 1000L / Math.max(drainMs, 1), String.join(",", samples));
            Files.createDirectories(Path.of("build/reports"));
            Files.writeString(Path.of("build/reports/lag.json"), result);
            System.out.printf("[lag] events=%d publishedMs=%d drainMs=%d lagAtEnd=%d appliedAtEnd=%d budgetMs=%d%n",
                    events.size(), publishedMs, drainMs, lag, applied, BUDGET_MS);

            assertThat(lag).as("lag never reached 0").isZero();
            assertThat(drainMs).isLessThanOrEqualTo(BUDGET_MS);
            assertThat(lab.ledger.appliedCount()).isEqualTo(events.size());
        }
    }
}
