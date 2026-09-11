package org.cris.prs.messaging.solace.schema;

import org.apache.avro.util.ClassSecurityValidator;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tells Avro which classes it may instantiate by name.
 *
 * <p>Since 1.11.4 Avro refuses to load a class named in a schema unless it is trusted
 * ({@code ClassSecurityValidator}), which is what stops a hostile schema naming a gadget class. Avro
 * reflection and specific records both look classes up that way, so without this a shared DTO fails with
 * <em>"Forbidden cris.prs.messaging.Person! This class is not trusted to be included in Avro schemas"</em>.
 * Avro's own switches are the JVM system properties {@code org.apache.avro.SERIALIZABLE_CLASSES} and
 * {@code SERIALIZABLE_PACKAGES}.</p>
 *
 * <p>This adds, once, a predicate to Avro's global validator &mdash; composed with whatever was there, so
 * the system properties keep working &mdash; that trusts:</p>
 * <ul>
 *   <li>every class the application itself hands to the codec: the class of each payload it sends, and each
 *       type a listener asks for. The application already uses those classes, so trusting them opens nothing
 *       a hostile schema could reach;</li>
 *   <li>the packages in {@code solace.schema-registry.avro.trusted-packages}, for classes the codec never
 *       sees directly &mdash; the types of a DTO's nested fields.</li>
 * </ul>
 *
 * <p>Avro's validator is JVM-wide, so so is this.</p>
 */
final class AvroClassTrust {

    private static final Set<String> CLASSES = ConcurrentHashMap.newKeySet();

    private static final Set<String> PACKAGES = ConcurrentHashMap.newKeySet();

    private static final AtomicBoolean INSTALLED = new AtomicBoolean();

    private AvroClassTrust() {
    }

    /**
     * Trust every class in these packages and their sub-packages.
     *
     * @param packages package names, such as {@code com.example.dto}; blank entries are ignored
     */
    static void trustPackages(Collection<String> packages) {
        for (String name : packages) {
            if (name != null && !name.isBlank()) {
                String trimmed = name.trim();
                PACKAGES.add(trimmed.endsWith(".") ? trimmed : trimmed + ".");
            }
        }
        install();
    }

    /**
     * Trust one class the application uses as a payload or a listener type.
     *
     * @param type the class; {@code null}, primitives, arrays' primitive components and {@code Object} are
     *             ignored
     */
    static void trust(Class<?> type) {
        Class<?> component = type;
        while (component != null && component.isArray()) {
            component = component.getComponentType();
        }
        if (component == null || component.isPrimitive() || component == Object.class) {
            return;
        }
        if (CLASSES.add(component.getName())) {
            install();
        }
    }

    /**
     * Whether a class is trusted by this predicate alone, ignoring Avro's defaults and system properties.
     *
     * @param type the class
     * @return {@code true} if trusted here
     */
    static boolean isTrusted(Class<?> type) {
        String name = type.getName();
        if (CLASSES.contains(name)) {
            return true;
        }
        for (String prefix : PACKAGES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static void install() {
        if (INSTALLED.compareAndSet(false, true)) {
            ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(
                    ClassSecurityValidator.getGlobal(), AvroClassTrust::isTrusted));
        }
    }
}
