package org.cris.prs.messaging.solace.schema;

import io.apicurio.registry.serde.AbstractDeserializer;
import io.apicurio.registry.serde.AbstractSerializer;
import io.apicurio.registry.serde.config.SerdeConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base for the Apicurio-backed codecs: owns serializer and deserializer lifecycles.
 *
 * <p>Serializers and deserializers are created and configured <em>lazily</em>, on first use.
 * Configuring one creates a registry client, and doing that at startup would stop every instance
 * starting while the registry is down &mdash; the opposite of what the schema cache is for. A problem in
 * the Apicurio configuration therefore surfaces on the first message; the settings themselves are
 * validated at startup.</p>
 *
 * <p>Each Apicurio instance is shared by every thread; Apicurio's serdes keep their caches in concurrent
 * maps for exactly that. Everything created is closed by {@link #close()}.</p>
 */
@Slf4j
public abstract class ApicurioSchemaCodec implements SchemaCodec {

    /** Settings the Apicurio configuration is derived from. */
    protected final SchemaRegistrySettings settings;

    private final List<AutoCloseable> created = new ArrayList<>();

    private volatile boolean closed;

    /**
     * Create a codec.
     *
     * @param settings validated settings
     */
    protected ApicurioSchemaCodec(SchemaRegistrySettings settings) {
        this.settings = settings;
    }

    /**
     * Configure a newly constructed serializer as a message-value serializer, and track it for closing.
     *
     * @param serializer a new, unconfigured Apicurio serializer
     * @param config     its configuration
     * @param <S>        the serializer type
     * @return the serializer, configured
     */
    protected <S extends AbstractSerializer<?, ?>> S configured(S serializer, Map<String, Object> config) {
        track(serializer);
        serializer.configure(new SerdeConfig(config), false);
        return serializer;
    }

    /**
     * Configure a newly constructed deserializer as a message-value deserializer, and track it for closing.
     *
     * @param deserializer a new, unconfigured Apicurio deserializer
     * @param config       its configuration
     * @param <D>          the deserializer type
     * @return the deserializer, configured
     */
    protected <D extends AbstractDeserializer<?, ?>> D configured(D deserializer, Map<String, Object> config) {
        track(deserializer);
        deserializer.configure(new SerdeConfig(config), false);
        return deserializer;
    }

    /**
     * The configuration for this codec's format: the common settings, then the format's own keys, then
     * the format's raw pass-through properties.
     *
     * @param formatSpecific the format's own keys
     * @param formatRaw      the format's raw properties
     * @return the complete configuration
     */
    protected Map<String, Object> configuration(Map<String, Object> formatSpecific, Map<String, String> formatRaw) {
        return ApicurioConfiguration.forFormat(this.settings, formatSpecific, formatRaw);
    }

    private void track(AutoCloseable closeable) {
        synchronized (this) {
            this.created.add(closeable);
        }
    }

    /**
     * A serde instance created on first use, at most once, and never after the codec is closed.
     *
     * <p>A creation that throws leaves nothing cached, so the next message tries again &mdash; which is
     * what a registry that was briefly unreachable at first use needs.</p>
     *
     * @param <T> the instance type
     */
    protected final class Lazy<T> {

        private final Supplier<T> factory;

        private volatile T value;

        /**
         * Create a lazily initialised reference.
         *
         * @param factory creates and configures the instance
         */
        protected Lazy(Supplier<T> factory) {
            this.factory = factory;
        }

        /**
         * The instance, creating it if this is the first use.
         *
         * @return the instance
         * @throws IllegalStateException if the codec has been closed
         */
        public T get() {
            T current = this.value;
            if (current == null) {
                synchronized (ApicurioSchemaCodec.this) {
                    if (ApicurioSchemaCodec.this.closed) {
                        throw new IllegalStateException("The " + getFormat() + " schema codec has been closed");
                    }
                    if (this.value == null) {
                        this.value = this.factory.get();
                    }
                    current = this.value;
                }
            }
            return current;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        List<AutoCloseable> toClose;
        synchronized (this) {
            if (this.closed) {
                return;
            }
            this.closed = true;
            toClose = new ArrayList<>(this.created);
            this.created.clear();
        }
        for (AutoCloseable closeable : toClose) {
            try {
                closeable.close();
            }
            catch (Exception ex) {
                log.warn("Failed to close an Apicurio {} serde", getFormat(), ex);
            }
        }
    }
}
