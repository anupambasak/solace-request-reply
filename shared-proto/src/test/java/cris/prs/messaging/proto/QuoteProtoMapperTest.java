package cris.prs.messaging.proto;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QuoteProtoMapperTest {

    @Test
    @DisplayName("a quote survives the round trip through Protobuf, bytes included")
    void quoteRoundTrip() throws Exception {
        Quote quote = new Quote("q-1", "Ada Lovelace", 2530.0, "INR", 1_700_000_000_000L);

        QuoteReply parsed = QuoteReply.parseFrom(QuoteProtoMapper.toReply(quote).toByteString());

        assertEquals(quote, QuoteProtoMapper.toQuote(parsed));
    }

    @Test
    @DisplayName("a person survives the round trip, and a null name becomes empty")
    void personRoundTrip() {
        Person person = new Person();
        person.setName("Grace Hopper");
        person.setAge(42);

        assertEquals(person, QuoteProtoMapper.toPerson(QuoteProtoMapper.toRequest(person)));

        Person unnamed = new Person();
        assertEquals("", QuoteProtoMapper.toRequest(unnamed).getPersonName());
    }
}
