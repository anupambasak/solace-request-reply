package org.cris.prs.messaging.solace.schema;

import org.cris.prs.messaging.solace.core.SolaceMessagingException;

import java.io.EOFException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * A payload could not be converted through the schema registry.
 *
 * <p>Carries a {@link Reason} so that an error handler can tell a failure a retry might fix &mdash; the
 * registry was unreachable &mdash; from one it never will: a payload that does not match its schema.
 * {@link SchemaRegistryErrorHandler} rejects the second kind straight to the dead message queue.</p>
 */
public class SchemaRegistryConversionException extends SolaceMessagingException {

    /** Why the conversion failed, and whether retrying could help. */
    public enum Reason {

        /**
         * The registry could not be reached, did not answer in time, answered 5xx or 429, or refused the
         * credentials (401/403 &mdash; a configuration fault, not a message fault). Retryable.
         */
        REGISTRY_UNAVAILABLE(true),

        /** The registry has no artifact for the id the body carries, or for the topic being sent to. */
        SCHEMA_NOT_FOUND(false),

        /** The payload does not conform to its schema, or its bytes do not decode against it. */
        VALIDATION_FAILED(false),

        /**
         * A message on a registry-governed destination is not registry-framed, and
         * {@code require-schema-id} is on.
         */
        MISSING_SCHEMA_ID(false),

        /** The message is in a format that is not enabled, or does not say which format it is. */
        UNSUPPORTED_FORMAT(false),

        /** The payload is not of a type the format can produce, or the listener asked for. */
        TYPE_MISMATCH(false),

        /**
         * The serde failed in a way this library does not recognise. Treated as retryable, so an error
         * handler defers to the container's configured outcome rather than rejecting a message that might
         * have been fine.
         */
        UNCLASSIFIED(true);

        private final boolean retryable;

        Reason(boolean retryable) {
            this.retryable = retryable;
        }

        /**
         * Whether a redelivery could plausibly succeed.
         *
         * @return {@code false} when the message will fail the same way however often it is retried
         */
        public boolean isRetryable() {
            return this.retryable;
        }
    }

    private final Reason reason;

    /**
     * Create an exception.
     *
     * @param reason  why the conversion failed
     * @param message description of what was being converted
     * @param cause   the underlying serde or Jackson failure; may be {@code null}
     */
    public SchemaRegistryConversionException(Reason reason, String message, Throwable cause) {
        super("[" + reason + "] " + message, cause);
        this.reason = reason;
    }

    /**
     * Why the conversion failed.
     *
     * @return the reason; never {@code null}
     */
    public Reason getReason() {
        return this.reason;
    }

    /**
     * Wrap a serde failure, classifying it by what its cause chain contains.
     *
     * <p>A {@code SchemaRegistryConversionException} already in the chain &mdash; thrown by
     * {@link SolaceTopicProfileStrategy} from inside Apicurio, say &mdash; keeps its reason. Otherwise, in
     * order: an HTTP status from the registry client; a validation or decoding failure; a not-found; anything
     * network-shaped. Everything else is {@link Reason#UNCLASSIFIED} &mdash; deliberately retryable, because
     * rejecting a good message to the dead message queue is worse than redelivering a bad one until
     * {@code max-redelivery-count} does the same.</p>
     *
     * @param message description of what was being converted
     * @param cause   what the serde threw
     * @return the classified exception; the one already in the chain, when there is one
     */
    public static SchemaRegistryConversionException classify(String message, Throwable cause) {
        SchemaRegistryConversionException existing = SchemaRegistryErrorHandler.find(cause);
        if (existing != null) {
            return existing;
        }
        return new SchemaRegistryConversionException(reasonOf(cause), message, cause);
    }

    static Reason reasonOf(Throwable cause) {
        Integer status = firstStatus(cause);
        if (status != null) {
            if (status == 404) {
                return Reason.SCHEMA_NOT_FOUND;
            }
            if (status >= 500 || status == 429 || status == 401 || status == 403) {
                return Reason.REGISTRY_UNAVAILABLE;
            }
            if (status == 400 || status == 409 || status == 422) {
                return Reason.VALIDATION_FAILED;
            }
        }
        if (anyInChain(cause, SchemaRegistryConversionException::isValidationFailure)) {
            return Reason.VALIDATION_FAILED;
        }
        if (anyInChain(cause, SchemaRegistryConversionException::isNotFound)) {
            return Reason.SCHEMA_NOT_FOUND;
        }
        if (anyInChain(cause, SchemaRegistryConversionException::isUnavailable)) {
            return Reason.REGISTRY_UNAVAILABLE;
        }
        return Reason.UNCLASSIFIED;
    }

    /**
     * The HTTP status of the first registry-client exception in the chain. Apicurio's client is generated
     * with Kiota, whose {@code ApiException} exposes {@code getResponseStatusCode()}; it is read reflectively
     * so this class needs no Kiota dependency.
     */
    private static Integer firstStatus(Throwable cause) {
        Throwable current = cause;
        int depth = 0;
        while (current != null && depth++ < 20) {
            try {
                Method method = current.getClass().getMethod("getResponseStatusCode");
                Object value = method.invoke(current);
                if (value instanceof Integer code && code > 0) {
                    return code;
                }
            }
            catch (ReflectiveOperationException | RuntimeException ignored) {
                // not a registry-client exception
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return null;
    }

    private static boolean isValidationFailure(Throwable t) {
        String name = t.getClass().getName();
        String text = messageOf(t);
        return name.endsWith("ValidationException") || name.startsWith("org.apache.avro.")
                || name.equals("com.google.protobuf.InvalidProtocolBufferException")
                || t instanceof EOFException
                || text.contains("error validating data")                // JSON Schema
                || text.contains("not compatible with the schema")       // Protobuf
                || text.contains("missing message type")                 // Protobuf
                || text.contains("unknown magic byte");                  // not Apicurio framing
    }

    private static boolean isNotFound(Throwable t) {
        String text = messageOf(t);
        return t.getClass().getSimpleName().contains("NotFound") || text.contains("not found")
                || text.contains("no artifact");
    }

    private static boolean isUnavailable(Throwable t) {
        String simpleName = t.getClass().getSimpleName();
        return t instanceof ConnectException || t instanceof UnknownHostException
                || t instanceof NoRouteToHostException || t instanceof SocketTimeoutException
                || t instanceof HttpTimeoutException || t instanceof TimeoutException
                || simpleName.contains("Timeout") || simpleName.contains("Connect");
    }

    private static String messageOf(Throwable t) {
        return t.getMessage() == null ? "" : t.getMessage().toLowerCase(Locale.ROOT);
    }

    private static boolean anyInChain(Throwable cause, Predicate<Throwable> test) {
        Throwable current = cause;
        int depth = 0;
        while (current != null && depth++ < 20) {
            if (test.test(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }
}
