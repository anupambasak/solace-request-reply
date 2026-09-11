package cris.prs.messaging.proto;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;

/**
 * Maps between the shared DTOs and the Protobuf demo's generated messages.
 *
 * <p>Protobuf, unlike Avro, has no reflection mode: what goes on the wire must be a generated
 * {@code Message}. So the services keep working in {@link Person} and {@link Quote} and convert at the
 * edge &mdash; the client before sending and after receiving, the server on either side of its listener.</p>
 *
 * <p>Proto3 has no null for strings; a {@code null} field becomes the empty string on the way out, and
 * stays empty on the way back.</p>
 */
public final class QuoteProtoMapper {

    private QuoteProtoMapper() {
    }

    /**
     * The request for a person's quote.
     *
     * @param person who the quote is for
     * @return the Protobuf request
     */
    public static QuoteRequest toRequest(Person person) {
        return QuoteRequest.newBuilder()
                .setPersonName(nullToEmpty(person.getName()))
                .setAge(person.getAge())
                .build();
    }

    /**
     * The person a request is for.
     *
     * @param request the Protobuf request
     * @return the shared DTO
     */
    public static Person toPerson(QuoteRequest request) {
        Person person = new Person();
        person.setName(request.getPersonName());
        person.setAge(request.getAge());
        return person;
    }

    /**
     * A quote as a Protobuf reply.
     *
     * @param quote the shared DTO
     * @return the Protobuf reply
     */
    public static QuoteReply toReply(Quote quote) {
        return QuoteReply.newBuilder()
                .setId(nullToEmpty(quote.getId()))
                .setPersonName(nullToEmpty(quote.getPersonName()))
                .setAmount(quote.getAmount())
                .setCurrency(nullToEmpty(quote.getCurrency()))
                .setQuotedAt(quote.getQuotedAt())
                .build();
    }

    /**
     * A Protobuf reply as the shared DTO.
     *
     * @param reply the Protobuf reply
     * @return the quote
     */
    public static Quote toQuote(QuoteReply reply) {
        return new Quote(reply.getId(), reply.getPersonName(), reply.getAmount(), reply.getCurrency(),
                reply.getQuotedAt());
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
