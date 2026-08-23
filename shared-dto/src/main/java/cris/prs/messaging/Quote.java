package cris.prs.messaging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reply payload of the second request-reply service.
 *
 * <p>Deliberately a different type from the request: it shows that the reply type is chosen per
 * call, {@code sendAndReceive(topic, person, Quote.class)}, and that one client can have replies of
 * several types outstanding at once &mdash; the correlation id, not the type, is what matches a
 * reply to its request.</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Quote {

    private String id;

    /** Who the quote was produced for. */
    private String personName;

    private double amount;

    private String currency;

    /** Millisecond epoch at which the quote was produced. */
    private long quotedAt;
}
