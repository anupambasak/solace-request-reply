package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import cris.prs.messaging.proto.QuoteReply;
import cris.prs.messaging.proto.QuoteRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Avro, Protobuf and JSON Schema quote services answer exactly as the plain JSON one does; only the
 * wire format differs, and that is the converter's business, not the listener's.
 */
class SchemaQuoteConsumersTest {

    @Test
    @DisplayName("the Avro service quotes on the shared DTOs")
    void avro() {
        Person person = new Person();
        person.setName("Ada Lovelace");
        person.setAge(36);

        Quote quote = new QuoteAvroConsumer().quote(person, "corr-1");

        assertThat(quote.getPersonName()).isEqualTo("Ada Lovelace");
        assertThat(quote.getAmount()).isEqualTo(QuotePricing.premiumFor(person));
        assertThat(quote.getCurrency()).isEqualTo("INR");
    }

    @Test
    @DisplayName("the JSON Schema service quotes on the shared DTOs")
    void jsonSchema() {
        Person person = new Person();
        person.setName("Alan Turing");
        person.setAge(41);

        Quote quote = new QuoteJsonSchemaConsumer().quote(person, "corr-3");

        assertThat(quote.getPersonName()).isEqualTo("Alan Turing");
        assertThat(quote.getAmount()).isEqualTo(QuotePricing.premiumFor(person));
        assertThat(quote.getCurrency()).isEqualTo("INR");
        assertThat(quote.getId()).isNotBlank();
    }

    @Test
    @DisplayName("the Protobuf service answers the generated request with the generated reply")
    void protobuf() {
        QuoteRequest request = QuoteRequest.newBuilder().setPersonName("Grace Hopper").setAge(40).build();

        QuoteReply reply = new QuoteProtobufConsumer().quote(request, "corr-2");

        assertThat(reply.getPersonName()).isEqualTo("Grace Hopper");
        assertThat(reply.getAmount()).isEqualTo(1_000 + 40 * 42.5);
        assertThat(reply.getId()).isNotBlank();
        assertThat(reply.getQuotedAt()).isPositive();
    }
}
