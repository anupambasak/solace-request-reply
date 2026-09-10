package org.cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;

import java.util.Map;

/**
 * Maps Spring message headers onto Solace native fields and SDT user properties, and back.
 *
 * <p>The distinction matters on the broker: a value written to an SDT user property can be used in a
 * broker-side selector, while one written to a native field such as the correlation id cannot.</p>
 *
 * <p>Implementations must be thread safe.</p>
 *
 * @see DefaultSolaceHeaderMapper
 * @see SolaceHeaders
 */
public interface SolaceHeaderMapper {

    /**
     * Apply headers to an outbound message.
     *
     * <p>Implementations should skip framework-internal headers rather than writing them to the
     * wire &mdash; see {@link DefaultSolaceHeaderMapper} for the set the default mapper ignores.</p>
     *
     * @param headers the headers to apply; may be {@code null} or empty
     * @param message the message to apply them to
     */
    void fromHeaders(Map<String, Object> headers, XMLMessage message);

    /**
     * Read a received message's native fields and user properties into a header map.
     *
     * @param message the received message
     * @return a mutable map of headers; never {@code null}
     */
    Map<String, Object> toHeaders(BytesXMLMessage message);
}
