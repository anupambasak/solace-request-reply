package cris.prs.messaging.consumer;

import cris.prs.messaging.InventoryCheck;
import cris.prs.messaging.InventoryStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Third request-reply service: inventory checks. */
class InventoryConsumerTest {

    private final InventoryConsumer consumer = new InventoryConsumer();

    @Test
    @DisplayName("answers for the SKU in the request")
    void answersForTheRequestedSku() {
        InventoryStatus status = consumer.check(new InventoryCheck("SKU-1", 5), "corr-1");

        assertThat(status.getSku()).isEqualTo("SKU-1");
        assertThat(status.getAvailable()).isNotNegative();
        assertThat(status.getWarehouse()).isEqualTo("WH-1");
        assertThat(status.getCheckedAt()).isPositive();
    }

    @Test
    @DisplayName("reports the same count for the same SKU, so a repeated check is repeatable")
    void isRepeatableForASku() {
        assertThat(consumer.check(new InventoryCheck("SKU-1", 1), null).getAvailable())
                .isEqualTo(consumer.check(new InventoryCheck("SKU-1", 1), null).getAvailable());
    }

    @Test
    @DisplayName("inStock compares the count against the quantity asked for")
    void inStockComparesAgainstTheRequestedQuantity() {
        int available = consumer.check(new InventoryCheck("SKU-2", 1), null).getAvailable();

        assertThat(consumer.check(new InventoryCheck("SKU-2", available), null).isInStock()).isTrue();
        assertThat(consumer.check(new InventoryCheck("SKU-2", available + 1), null).isInStock()).isFalse();
    }
}
