package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import cris.prs.messaging.solace.transaction.SolaceResourceHolder;
import cris.prs.messaging.solace.transaction.SolaceTransactionUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * The Solace counterpart of {@code KafkaTemplate}: converts a payload, applies headers and
 * publishes it, transparently joining any Solace transaction bound to the calling thread.
 *
 * @param <T> the default payload type
 */
@Slf4j
public class SolaceTemplate<T> implements SolaceOperations<T> {

    @Getter
    private final SolaceSessionFactory sessionFactory;

    @Getter
    @Setter
    private SolaceMessageConverter messageConverter;

    @Getter
    @Setter
    private SolaceHeaderMapper headerMapper = new DefaultSolaceHeaderMapper();

    /** Destination used by {@link #send(Object)}. */
    @Getter
    @Setter
    private String defaultDestination;

    @Getter
    @Setter
    private DeliveryMode deliveryMode = DeliveryMode.PERSISTENT;

    /** Message expiry in milliseconds; {@code 0} means never expire. */
    @Getter
    @Setter
    private long timeToLive = 0L;

    @Getter
    @Setter
    private Integer priority;

    @Getter
    @Setter
    private boolean dmqEligible = true;

    public SolaceTemplate(SolaceSessionFactory sessionFactory, SolaceMessageConverter messageConverter) {
        Assert.notNull(sessionFactory, "'sessionFactory' must not be null");
        Assert.notNull(messageConverter, "'messageConverter' must not be null");
        this.sessionFactory = sessionFactory;
        this.messageConverter = messageConverter;
    }

    @Override
    public void send(T payload) {
        Assert.state(StringUtils.hasText(this.defaultDestination),
                "No destination given and no 'defaultDestination' configured");
        send(this.defaultDestination, payload);
    }

    @Override
    public void send(String destination, T payload) {
        send(destination, payload, null);
    }

    @Override
    public void send(String destination, String correlationId, T payload) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(SolaceHeaders.CORRELATION_ID, correlationId);
        send(destination, payload, headers);
    }

    @Override
    public void send(String destination, T payload, Map<String, Object> headers) {
        XMLMessage message = createMessage(payload, headers);
        send(DefaultSolaceHeaderMapper.toDestination(destination), message);
    }

    @Override
    public void send(Message<?> message) {
        Map<String, Object> headers = DefaultSolaceHeaderMapper.sanitize(message.getHeaders());
        Object target = headers.remove(SolaceHeaders.TARGET_DESTINATION);
        String destination = target != null ? target.toString() : this.defaultDestination;
        Assert.state(StringUtils.hasText(destination), "No '" + SolaceHeaders.TARGET_DESTINATION
                + "' header and no 'defaultDestination' configured");
        XMLMessage solaceMessage = createMessage(message.getPayload(), headers);
        send(DefaultSolaceHeaderMapper.toDestination(destination), solaceMessage);
    }

    @Override
    public void send(Destination destination, XMLMessage message) {
        try {
            producer().send(message, destination);
            if (log.isTraceEnabled()) {
                log.trace("Published message correlationId={} to {}", message.getCorrelationId(),
                        destination.getName());
            }
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException("Unable to publish to " + destination.getName(), ex);
        }
    }

    @Override
    public <R> R executeInTransaction(TransactionCallback<T, R> callback) {
        SolaceResourceHolder existing = SolaceTransactionUtils.getActiveResourceHolder(this.sessionFactory);
        if (existing != null) {
            return callback.doInSolace(this);
        }
        SolaceResourceHolder holder = new SolaceResourceHolder(this.sessionFactory.createTransactedSession());
        holder.setSynchronizedWithTransaction(true);
        SolaceTransactionUtils.bindResourceHolder(this.sessionFactory, holder);
        try {
            R result = callback.doInSolace(this);
            holder.commit();
            return result;
        }
        catch (RuntimeException ex) {
            holder.rollback();
            throw ex;
        }
        finally {
            SolaceTransactionUtils.unbindResourceHolder(this.sessionFactory);
            holder.closeIfOwned();
        }
    }

    /** Build a Solace message for the given payload, applying the template defaults and headers. */
    public XMLMessage createMessage(Object payload, Map<String, Object> headers) {
        XMLMessage message = this.messageConverter.toMessage(payload);
        message.setDeliveryMode(this.deliveryMode);
        message.setDMQEligible(this.dmqEligible);
        if (this.timeToLive > 0) {
            message.setTimeToLive(this.timeToLive);
        }
        if (this.priority != null) {
            message.setPriority(this.priority);
        }
        if (headers != null && !headers.isEmpty()) {
            this.headerMapper.fromHeaders(headers, message);
        }
        return message;
    }

    /**
     * The producer to publish through: the transacted producer when a Solace transaction is active
     * on this thread, otherwise the shared session producer.
     */
    protected XMLMessageProducer producer() {
        SolaceResourceHolder holder = SolaceTransactionUtils.getActiveResourceHolder(this.sessionFactory);
        return holder != null ? holder.getProducer() : this.sessionFactory.getSharedProducer();
    }

    /** Whether a Solace transaction is currently bound to the calling thread. */
    public boolean isTransactionActive() {
        return SolaceTransactionUtils.getActiveResourceHolder(this.sessionFactory) != null;
    }
}
