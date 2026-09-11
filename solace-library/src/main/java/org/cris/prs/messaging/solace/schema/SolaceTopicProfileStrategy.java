package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.resolver.ParsedSchema;
import io.apicurio.registry.resolver.data.Record;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.resolver.strategy.ArtifactReferenceResolverStrategy;
import io.apicurio.registry.serde.data.SerdeMetadata;
import org.cris.prs.messaging.solace.support.SolaceTopicMatcher;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * An Apicurio artifact resolver strategy that maps Solace topic expressions to registry artifacts.
 *
 * <p>Apicurio's own strategies name the artifact after the topic ({@code SimpleTopicIdStrategy}, or
 * {@code TopicIdStrategy} with a {@code -value} suffix), which breaks on Solace's per-instance topics: every
 * pod's reply topic {@code app/reply/<instance-id>} would be a different artifact. This strategy matches
 * the topic against Solace wildcard expressions instead &mdash; {@code app/reply/>} &mdash; with the same
 * rules the broker uses (see {@link SolaceTopicMatcher}). First match wins, in declaration order.</p>
 *
 * <p>An unmatched topic is a {@link SchemaRegistryConversionException.Reason#SCHEMA_NOT_FOUND}: the payload
 * cannot be governed, and retrying will not change that.</p>
 *
 * @param <S> the schema type of the serde using the strategy
 */
public class SolaceTopicProfileStrategy<S> implements ArtifactReferenceResolverStrategy<S, Object> {

    private final List<SchemaRegistrySettings.TopicMapping> mappings;

    /**
     * Create a strategy.
     *
     * @param mappings topic expression to artifact mappings, tried in order
     */
    public SolaceTopicProfileStrategy(List<SchemaRegistrySettings.TopicMapping> mappings) {
        Assert.notNull(mappings, "'mappings' must not be null");
        this.mappings = List.copyOf(mappings);
    }

    /** {@inheritDoc} */
    @Override
    public ArtifactReference artifactReference(Record<Object> data, ParsedSchema<S> parsedSchema) {
        String topic = data.metadata() instanceof SerdeMetadata metadata ? metadata.getTopic() : null;
        SchemaRegistrySettings.TopicMapping mapping = match(topic);
        if (mapping == null) {
            throw new SchemaRegistryConversionException(SchemaRegistryConversionException.Reason.SCHEMA_NOT_FOUND,
                    "No solace.schema-registry.topic-profile mapping matches topic '" + topic + "'", null);
        }
        return ArtifactReference.builder()
                .groupId(StringUtils.hasText(mapping.getGroupId()) ? mapping.getGroupId() : null)
                .artifactId(mapping.getArtifactId())
                .version(StringUtils.hasText(mapping.getVersion()) ? mapping.getVersion() : null)
                .build();
    }

    /**
     * The schema is not needed to choose the artifact, so Apicurio need not extract it from the payload
     * first &mdash; the same choice its topic strategies make.
     *
     * @return {@code false}
     */
    @Override
    public boolean loadSchema() {
        return false;
    }

    /**
     * The first mapping whose expression matches a topic.
     *
     * @param topic the destination name; may be {@code null}
     * @return the mapping, or {@code null} if none matches
     */
    public SchemaRegistrySettings.TopicMapping match(String topic) {
        if (topic == null) {
            return null;
        }
        for (SchemaRegistrySettings.TopicMapping mapping : this.mappings) {
            if (SolaceTopicMatcher.matches(mapping.getTopicExpression(), topic)) {
                return mapping;
            }
        }
        return null;
    }
}
