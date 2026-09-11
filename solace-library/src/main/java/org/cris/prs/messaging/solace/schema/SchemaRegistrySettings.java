package org.cris.prs.messaging.solace.schema;

import lombok.Data;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Apicurio Registry settings, bound from {@code solace.schema-registry.*}.
 *
 * <p>Deliberately free of Apicurio types, so that binding it never needs the Apicurio jars; the codecs
 * translate it into Apicurio's {@code apicurio.registry.*} configuration keys.</p>
 *
 * <p>Every Apicurio-facing scalar is a nullable boxed type written to the Apicurio configuration only
 * when set, as {@code ContainerProperties.Flow} is for flow tuning: an untouched property keeps
 * Apicurio's default. Two defaults differ on purpose &mdash; {@code cache.fault-tolerant-refresh}
 * ({@code true}) and {@code http-adapter} ({@code JDK}); see {@link Cache} and {@code http-adapter}.</p>
 */
@Data
public class SchemaRegistrySettings {

    /** {@code artifact-resolver-strategy} value selecting {@link SolaceTopicProfileStrategy}. */
    public static final String STRATEGY_TOPIC_PROFILE = "TOPIC_PROFILE";

    /** {@code artifact-resolver-strategy} value selecting Apicurio's {@code SimpleTopicIdStrategy}. */
    public static final String STRATEGY_DESTINATION = "DESTINATION";

    /** {@code artifact-resolver-strategy} value selecting Apicurio's {@code TopicIdStrategy}. */
    public static final String STRATEGY_TOPIC = "TOPIC";

    /** {@code artifact-resolver-strategy} value selecting Apicurio's Avro {@code RecordIdStrategy}. */
    public static final String STRATEGY_RECORD = "RECORD";

    /** Create settings with every value at its documented default. */
    public SchemaRegistrySettings() {
    }

    /**
     * Apicurio Registry REST endpoint, for example {@code http://registry:8080/apis/registry/v3}. Setting
     * it is what enables the feature.
     */
    private String url;

    /**
     * The schema formats to enable. Empty &mdash; the default &mdash; enables every format whose Apicurio
     * serde module is on the classpath; a format listed here whose module is missing fails startup.
     */
    private List<SchemaFormat> formats = new ArrayList<>();

    /** Registry user name, for HTTP basic authentication. */
    private String username;

    /** Registry password, for HTTP basic authentication. */
    private String password;

    /** OAuth 2.0 client-credentials authentication, an alternative to basic authentication. */
    private final OAuth oauth = new OAuth();

    /** TLS towards the registry. */
    private final Tls tls = new Tls();

    /**
     * Apicurio's HTTP client: {@code JDK} (library default), {@code VERTX} or {@code AUTO}.
     *
     * <p>{@code JDK} is the default here, where Apicurio's is {@code AUTO} &mdash; which prefers Vert.x.
     * Every serializer and deserializer owns a client; with Vert.x each brings its own event loop, and its
     * Netty sits beside the one Spring WebFlux already uses. The JDK client needs neither.</p>
     */
    private HttpAdapter httpAdapter = HttpAdapter.JDK;

    /**
     * Solace topic expressions whose messages are governed by the registry.
     *
     * <p>Outbound, a POJO sent to a matching destination is serialised with JSON Schema through the
     * registry; elsewhere it falls back to plain JSON. An Avro record or a Protobuf message always goes
     * through the registry, since nothing else can serialise it. Inbound, a registry-framed body is always
     * decoded through the registry, whatever its destination. Empty &mdash; the default &mdash; means every
     * destination.</p>
     */
    private List<String> destinations = new ArrayList<>();

    /**
     * Reject a message on a governed destination whose body is not registry-framed, instead of falling
     * back to plain JSON. Off by default so producers can migrate one at a time.
     */
    private boolean requireSchemaId;

    /**
     * How a payload's schema artifact is found: {@code TOPIC_PROFILE} (default), {@code DESTINATION},
     * {@code TOPIC}, {@code RECORD} (Avro only), or the class name of any Apicurio
     * {@code ArtifactReferenceResolverStrategy}.
     *
     * <p>{@code TOPIC_PROFILE} is the default because it is the only one that handles per-instance reply
     * topics: map the reply prefix with {@code >}. {@code DESTINATION} and {@code TOPIC} make every
     * distinct topic its own artifact, so every pod's reply topic would be a different artifact.</p>
     */
    private String artifactResolverStrategy = STRATEGY_TOPIC_PROFILE;

    /** Topic expression to artifact mappings for {@code TOPIC_PROFILE}; first match wins. */
    private List<TopicMapping> topicProfile = new ArrayList<>();

    /** Resolve to the latest version of the artifact. Apicurio default: {@code false}. */
    private Boolean findLatest;

    /** Pin every serialisation to one artifact, overriding the strategy's group, id or version. */
    private final ExplicitArtifact explicitArtifact = new ExplicitArtifact();

    /**
     * Register a schema the registry does not have yet, on first serialisation. Apicurio default:
     * {@code false}. For development: in production, register schemas from CI or the registry UI.
     */
    private Boolean autoRegister;

    /** What auto-registration does when the artifact exists. Apicurio default: {@code FIND_OR_CREATE_VERSION}. */
    private IfArtifactExists autoRegisterIfExists;

    /**
     * Which registry id the body carries: {@code CONTENT_ID} (Apicurio default) or {@code GLOBAL_ID}.
     * Producers and consumers must agree.
     */
    private IdOption useId;

    /** Ask the registry for dereferenced schemas. Apicurio default: {@code false}. */
    private Boolean dereferenceSchema;

    /** Schema cache. */
    private final Cache cache = new Cache();

    /** Registry request retry. */
    private final Retry retry = new Retry();

    /** Avro-specific settings. */
    private final Avro avro = new Avro();

    /** Protobuf-specific settings. */
    private final Protobuf protobuf = new Protobuf();

    /** JSON Schema-specific settings. */
    private final JsonSchema jsonSchema = new JsonSchema();

    /**
     * Raw Apicurio configuration keys ({@code apicurio.registry.*}) for every format, applied after
     * everything above. The escape hatch for anything not modelled here.
     */
    private Map<String, String> properties = new LinkedHashMap<>();

    /**
     * Fail fast on a configuration that cannot work, naming what to change.
     *
     * @throws IllegalStateException if the configuration is incomplete or contradictory
     */
    public void validate() {
        if (!StringUtils.hasText(this.url)) {
            throw new IllegalStateException("solace.schema-registry.url must be set");
        }
        String strategy = this.artifactResolverStrategy;
        if (STRATEGY_RECORD.equalsIgnoreCase(strategy)
                && (this.formats.isEmpty() || this.formats.stream().anyMatch(f -> f != SchemaFormat.AVRO))) {
            throw new IllegalStateException("solace.schema-registry.artifact-resolver-strategy=RECORD is Avro "
                    + "only. Set solace.schema-registry.formats to [AVRO], or keep TOPIC_PROFILE and override "
                    + "Avro alone with solace.schema-registry.avro.properties"
                    + "[apicurio.registry.artifact-resolver-strategy]");
        }
        if ((strategy == null || STRATEGY_TOPIC_PROFILE.equalsIgnoreCase(strategy))
                && this.topicProfile.isEmpty()
                && !StringUtils.hasText(this.explicitArtifact.getArtifactId())) {
            throw new IllegalStateException("solace.schema-registry.topic-profile is empty, so no topic "
                    + "resolves to a schema. Add mappings (for example topic-expression: \"orders/>\", "
                    + "artifact-id: order), set explicit-artifact.artifact-id, or choose another "
                    + "artifact-resolver-strategy");
        }
        for (TopicMapping mapping : this.topicProfile) {
            if (!StringUtils.hasText(mapping.getTopicExpression())
                    || !StringUtils.hasText(mapping.getArtifactId())) {
                throw new IllegalStateException("Every solace.schema-registry.topic-profile entry needs a "
                        + "topic-expression and an artifact-id");
            }
        }
    }

    /** Apicurio's HTTP client implementation. */
    public enum HttpAdapter {
        /** Prefer Vert.x when present, else the JDK client (Apicurio's default). */
        AUTO,
        /** The JDK {@code HttpClient}. */
        JDK,
        /** Vert.x. */
        VERTX
    }

    /** Behaviour when auto-registering an artifact that already exists; mirrors Apicurio's values. */
    public enum IfArtifactExists {
        /** Fail the serialisation. */
        FAIL,
        /** Always add a new version. */
        CREATE_VERSION,
        /** Reuse a matching version, or add one. */
        FIND_OR_CREATE_VERSION
    }

    /** Which registry id is encoded in the body; mirrors Apicurio's {@code IdOption}. */
    public enum IdOption {
        /** The id of the schema content, shared by identical schemas. */
        CONTENT_ID,
        /** The id of the artifact version. */
        GLOBAL_ID
    }

    /** Avro body encoding; mirrors Apicurio's {@code AvroEncoding}. */
    public enum AvroEncoding {
        /** Avro's compact binary encoding. */
        BINARY,
        /** Avro's JSON encoding. */
        JSON
    }

    /** OAuth 2.0 client credentials. */
    @Data
    public static class OAuth {

        /** Create unset OAuth settings. */
        public OAuth() {
        }

        /** Token endpoint of the identity provider. */
        private String tokenEndpoint;

        /** Client id. */
        private String clientId;

        /** Client secret. */
        private String clientSecret;

        /** Requested scope. */
        private String scope;
    }

    /** TLS towards the registry. */
    @Data
    public static class Tls {

        /** Create unset TLS settings. */
        public Tls() {
        }

        /** Trust store location, for a registry with a private CA. */
        private String truststoreLocation;

        /** Trust store password. */
        private String truststorePassword;

        /** Trust store type. Apicurio default: {@code JKS}. */
        private String truststoreType;

        /** Key store location, for mutual TLS. */
        private String keystoreLocation;

        /** Key store password. */
        private String keystorePassword;

        /** Key store type. Apicurio default: {@code JKS}. */
        private String keystoreType;

        /** Trust every certificate. Development only. */
        private Boolean trustAll;

        /** Verify the registry's host name. Apicurio default: {@code true}. */
        private Boolean verifyHost;
    }

    /** One topic-expression-to-artifact mapping, for {@code TOPIC_PROFILE}. */
    @Data
    public static class TopicMapping {

        /** Create an empty mapping. */
        public TopicMapping() {
        }

        /** Solace topic expression, wildcards allowed: {@code orders/*}, {@code app/reply/>}. */
        private String topicExpression;

        /** Artifact group; Apicurio's default group when unset. */
        private String groupId;

        /** Artifact id. */
        private String artifactId;

        /** Artifact version; resolved by the lookup settings when unset. */
        private String version;
    }

    /** An artifact every serialisation is pinned to. */
    @Data
    public static class ExplicitArtifact {

        /** Create unset explicit artifact settings. */
        public ExplicitArtifact() {
        }

        /** Artifact group. */
        private String groupId;

        /** Artifact id. */
        private String artifactId;

        /** Artifact version. */
        private String version;
    }

    /**
     * Schema cache.
     *
     * <p>{@code faultTolerantRefresh} defaults to {@code true} here although Apicurio defaults it to
     * {@code false}: with Apicurio's default a registry outage fails every message once the cache period
     * lapses, while broker and application are both healthy. With it on, a cached schema keeps being used
     * and an outage means "no new schemas" rather than "no messages".</p>
     */
    @Data
    public static class Cache {

        /** Create cache settings with every value at its documented default. */
        public Cache() {
        }

        /** How long a resolved artifact is cached. Apicurio default: 30s. */
        private Duration checkPeriod;

        /** Cache "latest" lookups too. Apicurio default: {@code true}. */
        private Boolean latest;

        /** Keep serving a cached schema when refreshing it fails. Library default: {@code true}. */
        private Boolean faultTolerantRefresh = Boolean.TRUE;

        /** Serve stale entries while refreshing them in the background. Apicurio default: {@code false}. */
        private Boolean backgroundRefresh;
    }

    /** Registry request retry. */
    @Data
    public static class Retry {

        /** Create retry settings, unset. */
        public Retry() {
        }

        /** Retries per registry request. Apicurio default: 3. */
        private Long count;

        /** Wait between retries. Apicurio default: 300ms. */
        private Duration backoff;
    }

    /** Avro-specific settings. */
    @Data
    public static class Avro {

        /** Create Avro settings, unset. */
        public Avro() {
        }

        /** Body encoding. Apicurio default: {@code BINARY}. */
        private AvroEncoding encoding;

        /** Check a record's own schema against the registry's before writing. Apicurio default: {@code true}. */
        private Boolean validateWriterSchema;

        /** Raw Apicurio keys for the Avro serde only, applied last. */
        private Map<String, String> properties = new LinkedHashMap<>();
    }

    /** Protobuf-specific settings. */
    @Data
    public static class Protobuf {

        /** Create Protobuf settings, unset. */
        public Protobuf() {
        }

        /** Check a message's descriptor against the registry's schema before writing. Apicurio default: {@code true}. */
        private Boolean validation;

        /**
         * Derive the generated class from the schema's {@code java_outer_classname} and
         * {@code java_multiple_files}. Apicurio default: {@code false}, which yields a
         * {@code DynamicMessage} &mdash; the converter re-parses that into the listener's generated type
         * when it asks for one.
         */
        private Boolean deriveClass;

        /** Raw Apicurio keys for the Protobuf serde only, applied last. */
        private Map<String, String> properties = new LinkedHashMap<>();
    }

    /** JSON Schema-specific settings. */
    @Data
    public static class JsonSchema {

        /** Create JSON Schema settings, unset. */
        public JsonSchema() {
        }

        /** Validate payloads against the schema on both sides. Apicurio default: {@code true}. */
        private Boolean validation;

        /**
         * Classpath location of a schema, used when auto-registering &mdash; JSON Schema cannot be inferred
         * from a POJO, unlike Avro and Protobuf.
         */
        private String schemaLocation;

        /** Raw Apicurio keys for the JSON Schema serde only, applied last. */
        private Map<String, String> properties = new LinkedHashMap<>();
    }
}
