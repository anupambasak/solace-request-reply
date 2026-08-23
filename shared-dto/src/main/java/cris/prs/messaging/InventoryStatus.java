package cris.prs.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Reply payload of the inventory check service. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InventoryStatus {

    private String sku;

    /** Units on hand at the time of the check. */
    private int available;

    /** Whether {@link #available} met the quantity asked for. */
    private boolean inStock;

    /** Warehouse the count came from. */
    private String warehouse;

    /** Millisecond epoch at which the count was taken. */
    private long checkedAt;
}
