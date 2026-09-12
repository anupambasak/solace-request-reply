package cris.prs.messaging.spring.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cris.prs.messaging.Person;
import cris.prs.messaging.Quote;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The JSON Schema demo's contract lives in {@code shared-dto}'s {@code schemas/*.json} rather than in the
 * DTO classes, so nothing but a test keeps the two from drifting: a field added to {@link Person} or
 * {@link Quote} would otherwise be rejected by the registry only at runtime, on the first request.
 *
 * <p>They sit beside the DTOs they describe because both applications need them: each declares them under
 * {@code solace.schema-registry.registration.schemas}, and the library publishes them to Apicurio.</p>
 */
class JsonSchemaResourcesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("the request schema describes Person, as draft-07")
    void request() throws Exception {
        JsonNode schema = read("schemas/quote-jsonschema-request.json");

        assertThat(schema.path("$schema").asText()).contains("draft-07");
        assertThat(schema.path("type").asText()).isEqualTo("object");
        assertThat(properties(schema)).isEqualTo(fieldsOf(Person.class));
    }

    @Test
    @DisplayName("the reply schema describes Quote, as draft-07")
    void reply() throws Exception {
        JsonNode schema = read("schemas/quote-jsonschema-reply.json");

        assertThat(schema.path("$schema").asText()).contains("draft-07");
        assertThat(schema.path("type").asText()).isEqualTo("object");
        assertThat(properties(schema)).isEqualTo(fieldsOf(Quote.class));
    }

    private static JsonNode read(String location) throws Exception {
        try (var in = new ClassPathResource(location).getInputStream()) {
            return MAPPER.readTree(in);
        }
    }

    private static Set<String> properties(JsonNode schema) {
        Set<String> names = new LinkedHashSet<>();
        schema.path("properties").fieldNames().forEachRemaining(names::add);
        return names;
    }

    private static Set<String> fieldsOf(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
                .filter(field -> !field.isSynthetic() && !Modifier.isStatic(field.getModifiers()))
                .map(Field::getName)
                .collect(Collectors.toSet());
    }
}
