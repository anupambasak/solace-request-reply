package cris.prs.messaging.solace.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Resolves the instance id, in order, from an explicit override, the {@code HOSTNAME} or
 * {@code POD_NAME} environment variable (both set by Kubernetes), the local host name, and finally
 * a random suffix.
 *
 * <p>The result is sanitised so that it is safe to use as a single Solace topic level: characters
 * that carry meaning in topic strings ({@code /}, {@code *}, {@code >}) are replaced with
 * {@code -}.</p>
 */
@Slf4j
public class HostnameInstanceIdProvider implements InstanceIdProvider {

    private final String instanceId;

    public HostnameInstanceIdProvider() {
        this(null);
    }

    public HostnameInstanceIdProvider(String override) {
        this.instanceId = sanitize(resolve(override));
        log.info("Solace instance id resolved to '{}'", this.instanceId);
    }

    @Override
    public String getInstanceId() {
        return this.instanceId;
    }

    private static String resolve(String override) {
        if (StringUtils.hasText(override)) {
            return override;
        }
        String hostname = System.getenv("HOSTNAME");
        if (StringUtils.hasText(hostname)) {
            return hostname;
        }
        String podName = System.getenv("POD_NAME");
        if (StringUtils.hasText(podName)) {
            return podName;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (Exception ex) {
            log.warn("Unable to determine the local host name, falling back to a random instance id", ex);
            return "instance-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

    /** Make a value safe to embed in a single Solace topic level. */
    public static String sanitize(String value) {
        return value.replaceAll("[/*>\\s]", "-");
    }
}
