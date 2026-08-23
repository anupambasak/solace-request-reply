package cris.prs.messaging.consumer;

import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Second request-reply service: replies with a different type from the request. */
class QuoteConsumerTest {

    private final QuoteConsumer consumer = new QuoteConsumer();

    private static Person person(String name, int age) {
        Person person = new Person();
        person.setName(name);
        person.setAge(age);
        return person;
    }

    @Test
    @DisplayName("quotes for the person in the request")
    void quotesForTheRequestedPerson() {
        Quote quote = consumer.quote(person("Ada Lovelace", 36), "corr-1");

        assertThat(quote.getPersonName()).isEqualTo("Ada Lovelace");
        assertThat(quote.getId()).isNotBlank();
        assertThat(quote.getCurrency()).isEqualTo("INR");
        assertThat(quote.getQuotedAt()).isPositive();
    }

    @Test
    @DisplayName("derives the amount from the request, so the reply is not a constant")
    void derivesTheAmountFromTheRequest() {
        double younger = consumer.quote(person("A", 25), null).getAmount();
        double older = consumer.quote(person("B", 60), null).getAmount();

        assertThat(older).isGreaterThan(younger);
    }

    @Test
    @DisplayName("gives every quote a distinct id")
    void assignsADistinctIdPerQuote() {
        assertThat(consumer.quote(person("A", 30), null).getId())
                .isNotEqualTo(consumer.quote(person("A", 30), null).getId());
    }
}
