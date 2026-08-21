package cris.prs.messaging.solace.listener;

/**
 * Keeps the JVM alive while at least one listener container is running.
 *
 * <p>All of JCSMP's own threads are daemon threads, so a consumer-only Spring Boot application
 * (no web server) would otherwise return from {@code SpringApplication.run} and shut straight down
 * with its listeners perfectly healthy. Spring's JMS and Kafka containers avoid this by running
 * their listeners on non-daemon executor threads; this class does the same job with a single
 * reference counted thread shared by every container in the JVM.</p>
 */
final class ContainerKeepAlive {

    private static int count;

    private static Thread thread;

    private ContainerKeepAlive() {
    }

    static synchronized void acquire() {
        if (count++ == 0) {
            thread = new Thread(() -> {
                try {
                    // A thread joining itself parks until interrupted.
                    Thread.currentThread().join();
                }
                catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }, "solace-keep-alive");
            thread.setDaemon(false);
            thread.start();
        }
    }

    static synchronized void release() {
        if (count > 0 && --count == 0 && thread != null) {
            thread.interrupt();
            thread = null;
        }
    }
}
