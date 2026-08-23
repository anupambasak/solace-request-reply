package cris.prs.messaging.consumer;

import cris.prs.messaging.InventoryCheck;
import cris.prs.messaging.InventoryStatus;
import cris.prs.messaging.solace.annotation.SolaceListener;
import cris.prs.messaging.solace.core.SolaceHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Third request-reply service: inventory checks.
 *
 * <p>Bound to the durable queue {@code request-reply-queue-3.request-reply-group-3}, subscribed to
 * {@code request-reply/request-3} and {@code request-reply/request-3/>}.</p>
 *
 * <p><b>The responder is unaware of where its reply goes.</b> It returns a value and the container
 * publishes it to the destination in the request's {@code replyTo} field. This service's replies
 * therefore land on a <em>different</em> reply destination from the other two purely because the
 * client asked for them there &mdash; nothing here changes to make that happen, and nothing here
 * would need to change if the client went back to a shared one.</p>
 */
@Slf4j
@Component
public class InventoryConsumer {

    @SolaceListener(
            id = "inventory",
            pattern = "REQUEST_REPLY",
            queue = "${app.inventory.queue:request-reply-queue-3}",
            group = "${app.inventory.group:request-reply-group-3}",
            topics = {"${app.inventory.topic:request-reply/request-3}",
                    "${app.inventory.topic:request-reply/request-3}/>"},
            concurrency = "${app.inventory.concurrency:5}",
            transactional = "${app.inventory.transactional:true}")
    public InventoryStatus check(InventoryCheck request,
            @Header(name = SolaceHeaders.CORRELATION_ID, required = false) String correlationId) {
        log.debug("Handling inventory check correlationId={} payload={}", correlationId, request);
        int available = availableFor(request.getSku());
        InventoryStatus status = new InventoryStatus(request.getSku(), available,
                available >= request.getQuantity(), "WH-1", System.currentTimeMillis());
        log.debug("Inventory for {}: {} available, inStock={}", status.getSku(), status.getAvailable(),
                status.isInStock());
        return status;
    }

    /** A stand-in count: stable for a given SKU so a repeated check gives a repeatable answer. */
    private int availableFor(String sku) {
        return sku == null ? 0 : Math.floorMod(sku.hashCode(), 250);
    }
}
