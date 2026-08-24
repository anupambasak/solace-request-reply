package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
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

    /**
     * Create a template.
     *
     * @param sessionFactory supplies the connection and, as the transaction resource key, decides
     *                       which transactions this template joins
     * @param messageConverter serialises payloads into message bodies
     * @throws IllegalArgumentException if either argument is {@code null}
     */
    public SolaceTemplate(SolaceSessionFactory sessionFactory, SolaceMessageConverter messageConverter) {
        Assert.notNull(sessionFactory, "'sessionFactory' must not be null");
        Assert.notNull(messageConverter, "'messageConverter' must not be null");
        this.sessionFactory = sessionFactory;
        this.messageConverter = messageConverter;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Publishes to the configured {@code defaultDestination}.</p>
     */
    @Override
    public void send(T payload) {
        Assert.state(StringUtils.hasText(this.defaultDestination),
                "No destination given and no 'defaultDestination' configured");
        send(this.defaultDestination, payload);
    }

    /** {@inheritDoc} */
    @Override
    public void send(String destination, T payload) {
        send(destination, payload, null);
    }

    /** {@inheritDoc} */
    @Override
    public void send(String destination, String correlationId, T payload) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(SolaceHeaders.CORRELATION_ID, correlationId);
        send(destination, payload, headers);
    }

    /** {@inheritDoc} */
    @Override
    public void send(String destination, T payload, Map<String, Object> headers) {
        XMLMessage message = createMessage(payload, headers);
        send(DefaultSolaceHeaderMapper.toDestination(destination), message);
    }

    /** {@inheritDoc} */
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

    /**
     * {@inheritDoc}
     *
     * <p>The terminal publish that every other overload funnels into. Publishing is asynchronous:
     * this returns once JCSMP has accepted the message, and a broker-side failure is reported to
     * the producer's event handler rather than thrown from here.</p>
     */
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

    /**
     * {@inheritDoc}
     *
     * <p>When a transaction is already bound to this thread the callback simply runs inside it;
     * otherwise a transacted session is created, bound, committed and closed around the callback.</p>
     */
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

    /** {@inheritDoc} */
    @Override
    public <B> SolaceBrowser<B> browse(String queue, Class<B> payloadType) {
        return browse(BrowseSpec.of(queue), payloadType);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Browses on the shared session. A browser binds to the endpoint like a consumer, so it counts
     * against that endpoint's bind limit &mdash; an exclusive endpoint that already has its consumer
     * will reject one.</p>
     */
    @Override
    public <B> SolaceBrowser<B> browse(BrowseSpec spec, Class<B> payloadType) {
        Assert.notNull(spec, "'spec' must not be null");
        Assert.hasText(spec.getQueue(), "'queue' must not be empty");
        Assert.notNull(payloadType, "'payloadType' must not be null");

        BrowserProperties browserProperties = new BrowserProperties();
        browserProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(spec.getQueue()));
        if (StringUtils.hasText(spec.getSelector())) {
            browserProperties.setSelector(spec.getSelector());
        }
        if (spec.getTransportWindowSize() != null) {
            browserProperties.setTransportWindowSize(spec.getTransportWindowSize());
        }
        try {
            Browser browser = this.sessionFactory.getSharedSession().createBrowser(browserProperties);
            return new DefaultSolaceBrowser<>(browser, this.messageConverter, this.headerMapper,
                    payloadType, spec.getWaitTimeout(), spec.getQueue());
        }
        catch (JCSMPException ex) {
            throw new SolaceMessagingException(
                    "Unable to browse queue '" + spec.getQueue() + "'", ex);
        }
    }

    /**
     * Build a Solace message for the given payload, applying the template defaults.
     *
     * <p>Public so that a caller can pre-build a message once and publish it repeatedly through
     * {@link #send(Destination, XMLMessage)}, avoiding repeated serialisation.</p>
     *
     * @param payload the payload to serialise; may be {@code null}, producing an empty body
     * @param headers headers to apply; may be {@code null} or empty
     * @return a message with delivery mode, DMQ eligibility, expiry and priority applied
     */
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
     * The producer to publish through.
     *
     * <p>The transacted producer when a Solace transaction is active on this thread, otherwise the
     * shared session producer. This single decision is what makes every {@code send} transactional
     * inside {@code @Transactional} code without a separate API.</p>
     *
     * @return the producer appropriate to the current thread's transaction context
     */
    protected XMLMessageProducer producer() {
        SolaceResourceHolder holder = SolaceTransactionUtils.getActiveResourceHolder(this.sessionFactory);
        return holder != null ? holder.getProducer() : this.sessionFactory.getSharedProducer();
    }

    /**
     * Whether a Solace transaction is currently bound to the calling thread.
     *
     * @return {@code true} if the next {@code send} would join a transaction rather than publish
     *         immediately
     */
    public boolean isTransactionActive() {
        return SolaceTransactionUtils.getActiveResourceHolder(this.sessionFactory) != null;
    }
}
