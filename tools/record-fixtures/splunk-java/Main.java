import ch.qos.logback.classic.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Logs three lines through the HEC appenders in logback.xml: INFO whose message is a JSON object
 * (so the `messageFormat=json` appender embeds it as an object), WARN with an MDC value, and
 * ERROR. Then it waits for the appenders' asynchronous sends and stops Logback, which flushes and
 * closes them.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger("splunk-java-fixture");
        log.info("{\"action\":\"start\",\"component\":\"splunk-java fixture\",\"port\":8080}");
        MDC.put("request_id", "req-0001");
        log.warn("splunk-java fixture: slow upstream");
        MDC.clear();
        log.error("splunk-java fixture: request failed");
        Thread.sleep(3000);
        ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
    }
}
