/**
 * Schema registry support backed by <a href="https://www.apicur.io/registry/">Apicurio Registry</a>:
 * a {@code SolaceMessageConverter} that serialises and validates payloads against schemas held in the
 * registry, in three formats &mdash; Apache Avro, Google Protocol Buffers and JSON Schema.
 *
 * <p>Built on Apicurio's <em>generic</em> serde modules ({@code apicurio-registry-serde-common-avro},
 * {@code -protobuf}, {@code -jsonschema}), which have no Kafka dependency. The wire format is Apicurio's
 * standard one &mdash; a magic byte and the schema id in front of the encoded body &mdash; so a message
 * produced here can be read by any Apicurio consumer, and the reverse.</p>
 *
 * <p>This is the only package that imports {@code io.apicurio}, Apache Avro or Protocol Buffers, and only
 * the {@link org.cris.prs.messaging.solace.schema.SchemaCodec} implementations and
 * {@link org.cris.prs.messaging.solace.schema.SolaceTopicProfileStrategy} do. Everything else here, the
 * converter included, works against the {@code SchemaCodec} interface, so the Apicurio jars stay an
 * optional dependency and the routing logic can be tested without them.</p>
 *
 * <p>Enabled by setting {@code solace.schema-registry.url}; see
 * {@link org.cris.prs.messaging.solace.schema.SchemaRegistrySettings}.</p>
 */
package org.cris.prs.messaging.solace.schema;
