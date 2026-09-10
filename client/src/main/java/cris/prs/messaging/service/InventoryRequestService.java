package cris.prs.messaging.service;

import cris.prs.messaging.InventoryCheck;
import cris.prs.messaging.InventoryStatus;
import org.cris.prs.messaging.solace.requestreply.ReplyingSolaceTemplate;
import org.cris.prs.messaging.solace.requestreply.RequestReplyFuture;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Request-reply producer for the inventory service.
 *
 * <p>The only thing that distinguishes it from the other two producers is the template it is given:
 * {@code inventoryReplyingSolaceTemplate} stamps requests with its own reply destination, so
 * inventory replies arrive on an endpoint of their own rather than the shared one.</p>
 */
@Service
public class InventoryRequestService {

    private final ReplyingSolaceTemplate solace;

    private final String requestTopic;

    /**
     * @param solace       the inventory template, qualified because more than one
     *                     {@code ReplyingSolaceTemplate} exists once a service has its own reply
     *                     destination
     * @param requestTopic the inventory request topic
     */
    public InventoryRequestService(
            @Qualifier("inventoryReplyingSolaceTemplate") ReplyingSolaceTemplate solace,
            @Value("${app.inventory.topic:request-reply/request-3}") String requestTopic) {
        this.solace = solace;
        this.requestTopic = requestTopic;
    }

    /** Publish one inventory check and return a future for its reply. */
    public RequestReplyFuture<InventoryStatus> send(InventoryCheck check) {
        return this.solace.sendAndReceive(this.requestTopic, check, InventoryStatus.class);
    }

    /** Publish several inventory checks as independent publishes. */
    public List<RequestReplyFuture<InventoryStatus>> sendMultiple(List<InventoryCheck> checks) {
        return checks.stream().map(this::send).toList();
    }

    /**
     * Publish several inventory checks in one Solace local transaction.
     *
     * @return futures for the replies, to be awaited after this method has returned
     */
    @Transactional
    public List<RequestReplyFuture<InventoryStatus>> sendBatchInTransaction(List<InventoryCheck> checks) {
        return checks.stream().map(this::send).toList();
    }

    /** The destination inventory replies arrive on, distinct from the shared one. */
    public String getReplyDestination() {
        return this.solace.getReplyDestination();
    }
}
