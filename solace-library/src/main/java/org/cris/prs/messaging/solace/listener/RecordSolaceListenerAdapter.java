package org.cris.prs.messaging.solace.listener;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.cris.prs.messaging.solace.core.SolaceHeaderMapper;
import org.cris.prs.messaging.solace.core.SolaceMessageConverter;
import org.cris.prs.messaging.solace.core.SolaceRecord;

import java.util.Map;
import java.util.function.Function;

/**
 * Adapts a plain {@code Function<SolaceRecord<T>, R>} to the listener contract, for listeners
 * registered programmatically rather than through {@code @SolaceListener}. A non-null return value
 * is published as a reply.
 *
 * @param <T> inbound payload type
 * @param <R> reply payload type
 */
public class RecordSolaceListenerAdapter<T, R> extends AbstractSolaceListenerAdapter {

    private final Function<SolaceRecord<T>, R> handler;

    /**
     * Create an adapter around a record handler.
     *
     * @param handler          receives each record; a non-null return value is published as a reply
     * @param messageConverter converts the message body
     * @param headerMapper     maps native fields and user properties into headers
     */
    public RecordSolaceListenerAdapter(Function<SolaceRecord<T>, R> handler,
            SolaceMessageConverter messageConverter, SolaceHeaderMapper headerMapper) {
        super(messageConverter, headerMapper);
        this.handler = handler;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void onMessage(BytesXMLMessage message) {
        Object payload = convertPayload(message);
        Map<String, Object> headers = this.headerMapper.toHeaders(message);
        SolaceRecord<T> record = (SolaceRecord<T>) toRecord(payload, message, headers);
        handleResult(this.handler.apply(record), message);
    }
}
