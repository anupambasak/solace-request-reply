package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The payload source shared by all three pattern controllers. */
class PersonFactoryTest {

    private final PersonFactory factory = new PersonFactory();

    @Test
    @DisplayName("creates a person with a name and a plausible age")
    void createsAPerson() {
        Person person = factory.create();

        assertThat(person.getName()).isNotBlank();
        assertThat(person.getAge()).isBetween(18, 79);
    }

    @Test
    @DisplayName("creates the requested number of people")
    void createsManyPeople() {
        List<Person> people = factory.create(25);

        assertThat(people).hasSize(25).allSatisfy(p -> assertThat(p.getName()).isNotBlank());
    }
}
