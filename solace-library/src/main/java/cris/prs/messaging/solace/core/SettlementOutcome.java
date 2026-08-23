package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.XMLMessage;

/**
 * What a container does with a message whose listener threw.
 *
 * <p>Solace calls this <em>settling</em> a message. Until 10.17 a consumer had exactly one answer
 * &mdash; acknowledge, or say nothing &mdash; which meant a poison message could only be discarded or
 * left to be redelivered on the next bind. Settlement outcomes add the two answers that are actually
 * wanted: hand the message back for redelivery, or reject it outright.</p>
 *
 * <p>Choosing between them is a judgement about the <em>failure</em>, not about the message:</p>
 *
 * <table border="1">
 *   <caption>When each outcome is right</caption>
 *   <tr><th>Outcome</th><th>Broker behaviour</th><th>Use when</th></tr>
 *   <tr>
 *     <td>{@link #ACCEPTED}</td>
 *     <td>Acknowledged. Removed from the endpoint. Never redelivered.</td>
 *     <td>The failure has been recorded somewhere durable and retrying would not help.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #FAILED}</td>
 *     <td>Returned for redelivery, <strong>incrementing the delivery count</strong>. Moves to the
 *         dead message queue once {@code max-redelivery-count} is exhausted.</td>
 *     <td>A transient failure &mdash; a downstream timeout, a lock conflict &mdash; that a retry
 *         plausibly fixes.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #REJECTED}</td>
 *     <td>Straight to the dead message queue, <strong>without</strong> consuming redelivery
 *         attempts. Discarded if the message is not DMQ-eligible.</td>
 *     <td>The message will never succeed &mdash; it will not deserialise, it fails validation, it
 *         names something that does not exist. Retrying it four more times only delays the
 *         inevitable and holds up the flow.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #NONE}</td>
 *     <td>Nothing is sent. The message stays unacknowledged until the flow is rebound.</td>
 *     <td>Legacy behaviour, equivalent to {@code ack-on-error: false}. Prefer {@link #FAILED},
 *         which redelivers promptly and counts the attempt.</td>
 *   </tr>
 * </table>
 *
 * <h2>Requirements</h2>
 * <p>{@link #FAILED} and {@link #REJECTED} need JCSMP 10.17 or later and a broker that supports
 * settlement outcomes, and a flow must <strong>declare at bind time</strong> which outcomes it will
 * use. The container does that for you &mdash; see
 * {@code ContainerProperties.negativeAcknowledgement}.</p>
 *
 * <h2>Transacted flows</h2>
 * <p>Settlement does not apply to a transacted flow: the transaction's rollback already returns the
 * message for redelivery, and settling separately would conflict with it. A container with
 * {@code transactional = true} ignores this setting.</p>
 */
public enum SettlementOutcome {

    /** Acknowledge the message. It is removed from the endpoint and never redelivered. */
    ACCEPTED(XMLMessage.Outcome.ACCEPTED),

    /** Return the message for redelivery, incrementing its delivery count. */
    FAILED(XMLMessage.Outcome.FAILED),

    /** Send the message to the dead message queue without consuming redelivery attempts. */
    REJECTED(XMLMessage.Outcome.REJECTED),

    /** Settle nothing; the message stays unacknowledged until the flow is rebound. */
    NONE(null);

    private final XMLMessage.Outcome outcome;

    SettlementOutcome(XMLMessage.Outcome outcome) {
        this.outcome = outcome;
    }

    /**
     * The JCSMP outcome this maps to.
     *
     * @return the JCSMP {@code XMLMessage.Outcome}, or {@code null} for {@link #NONE}, which sends
     *         nothing
     */
    public XMLMessage.Outcome jcsmpOutcome() {
        return this.outcome;
    }

    /**
     * Whether this outcome must be declared on the flow at bind time.
     *
     * <p>{@link #ACCEPTED} is the ordinary acknowledgement every flow can already send, and
     * {@link #NONE} sends nothing; only the two negative outcomes have to be negotiated.</p>
     *
     * @return {@code true} for {@link #FAILED} and {@link #REJECTED}
     */
    public boolean requiresNegotiation() {
        return this == FAILED || this == REJECTED;
    }
}
