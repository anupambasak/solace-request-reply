package cris.prs.messaging.service;

import cris.prs.messaging.InventoryCheck;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.IntStream;

/** Generates the sample inventory requests the demo endpoints publish. */
@Component
public class InventoryCheckFactory {

    private final Faker faker = new Faker();

    /** A single random check. */
    public InventoryCheck create() {
        return new InventoryCheck(faker.commerce().promotionCode(),
                faker.number().numberBetween(1, 50));
    }

    /** {@code count} random checks. */
    public List<InventoryCheck> create(int count) {
        return IntStream.range(0, count).mapToObj(i -> create()).toList();
    }
}
