package cris.prs.messaging.service;

import cris.prs.messaging.Person;
import net.datafaker.Faker;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Generates the sample payloads the demo endpoints publish.
 *
 * <p>Shared by every controller so that the three patterns exercise identical data and their
 * latencies stay comparable.</p>
 */
@Component
public class PersonFactory {

    private final Faker faker = new Faker();

    /** A single random person. */
    public Person create() {
        Person person = new Person();
        person.setName(faker.name().fullName());
        person.setAge(faker.number().numberBetween(18, 80));
        return person;
    }

    /** {@code count} random people. */
    public List<Person> create(int count) {
        return IntStream.range(0, count).mapToObj(i -> create()).toList();
    }
}
