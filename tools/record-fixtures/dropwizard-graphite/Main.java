import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.PickledGraphite;
import java.util.concurrent.TimeUnit;

/**
 * Reports a fixed registry once through Dropwizard's `GraphiteReporter` and `PickledGraphite`,
 * so the capture holds one real protocol-0 frame. A fixed clock pins every datapoint's timestamp
 * to 1700000000; `GraphiteReporter` sends a registry's metrics in name order and formats each
 * value with `%2.2f`, so the frame is the same on every run.
 *
 * `interop_fixture_dropwizard_decodes` in crates/logit-inputs/src/graphite/mod.rs asserts on these
 * exact names and values; change them together.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "capture";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 2004;

        MetricRegistry registry = new MetricRegistry();
        registry.counter("requests").inc(42);
        registry.register("heap.used", (com.codahale.metrics.Gauge<Double>) () -> 12.5);
        registry.register("ratio.undefined", (com.codahale.metrics.Gauge<Double>) () -> Double.NaN);
        // A non-ASCII name: `PickledGraphite` writes it as raw UTF-8 inside `S'...'`, unescaped.
        registry.counter("café.visits").inc(3);

        Clock fixed = new Clock() {
            @Override
            public long getTick() {
                return 0;
            }

            @Override
            public long getTime() {
                return 1_700_000_000_000L;
            }
        };

        PickledGraphite graphite = new PickledGraphite(host, port);
        GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                .withClock(fixed)
                .prefixedWith("logit-fixture.dropwizard")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        // One report, which connects, sends one frame, and closes. Not `reporter.close()`: that
        // reports once more, on a second connection.
        reporter.report();
        System.out.println("dropwizard-graphite: reported " + registry.getMetrics().size()
                + " metrics through PickledGraphite to " + host + ":" + port);
    }
}
