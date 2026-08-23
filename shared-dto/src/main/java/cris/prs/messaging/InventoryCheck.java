package cris.prs.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Request payload of the inventory check service. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InventoryCheck {

    private String sku;

    /** How many units the caller wants. */
    private int quantity;
}
