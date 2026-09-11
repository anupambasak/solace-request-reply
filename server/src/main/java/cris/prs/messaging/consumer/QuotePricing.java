package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;

import java.util.UUID;

/**
 * The quote calculation shared by every quote service &mdash; JSON, Avro and Protobuf &mdash; so the
 * three differ only in how the payload travels, never in what it says.
 */
final class QuotePricing {

    private QuotePricing() {
    }

    /**
     * Quote for a person.
     *
     * @param person who the quote is for
     * @return a new quote with a fresh id, in INR, stamped now
     */
    static Quote quoteFor(Person person) {
        return new Quote(UUID.randomUUID().toString(), person.getName(), premiumFor(person), "INR",
                System.currentTimeMillis());
    }

    /** A stand-in calculation: enough to make the reply depend on the request. */
    static double premiumFor(Person person) {
        return Math.round((1_000 + person.getAge() * 42.5) * 100.0) / 100.0;
    }
}
