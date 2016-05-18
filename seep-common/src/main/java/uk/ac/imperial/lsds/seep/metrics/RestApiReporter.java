package uk.ac.imperial.lsds.seep.metrics;

import java.io.PrintStream;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;

/**
 * A reporter which outputs measurements to a {@link PrintStream}, like {@code System.out}.
 */
public class RestApiReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link ConsoleReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link ConsoleReporter}
     */
	
	private static LogicalOperator lo;
    public static Builder forRegistry(MetricRegistry registry, LogicalOperator o) {
    	lo = o;
        return new Builder(registry);
    }

    /**
     * A builder for {@link ConsoleReporter} instances. Defaults to using the default locale and
     * time zone, writing to {@code System.out}, converting rates to events/second, converting
     * durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private PrintStream output;
        private Locale locale;
        private Clock clock;
        private TimeZone timeZone;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.output = System.out;
            this.locale = Locale.getDefault();
            this.clock = Clock.defaultClock();
            this.timeZone = TimeZone.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Write to the given {@link PrintStream}.
         *
         * @param output a {@link PrintStream} instance.
         * @return {@code this}
         */
        public Builder outputTo(PrintStream output) {
            this.output = output;
            return this;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formattedFor(Locale locale) {
            this.locale = locale;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Use the given {@link TimeZone} for the time.
         *
         * @param timeZone a {@link TimeZone}
         * @return {@code this}
         */
        public Builder formattedFor(TimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link ConsoleReporter} with the given properties.
         *
         * @return a {@link ConsoleReporter}
         */
        public RestApiReporter build() {
            return new RestApiReporter(registry,
                                       output,
                                       locale,
                                       clock,
                                       timeZone,
                                       rateUnit,
                                       durationUnit,
                                       filter);
        }
    }

    private static final int CONSOLE_WIDTH = 80;

    private final PrintStream output;
    private final Locale locale;
    private final Clock clock;
    private final DateFormat dateFormat;

    private RestApiReporter(MetricRegistry registry,
                            PrintStream output,
                            Locale locale,
                            Clock clock,
                            TimeZone timeZone,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter) {
        super(registry, "console-reporter", filter, rateUnit, durationUnit);
        this.output = output;
        this.locale = locale;
        this.clock = clock;
        this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT,
                                                         DateFormat.MEDIUM,
                                                         locale);
        dateFormat.setTimeZone(timeZone);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
    	
    	if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            	if(entry.getKey().compareToIgnoreCase("uk.ac.imperial.lsds.seepworker.core.BufferPool.event.mem") == 0){
					try {
						sendPost("memory", Long.toString(entry.getValue().getCount()));
					} catch (Exception e) {
						e.printStackTrace();
					}
            	}
            }
        }

        if (!meters.isEmpty()) {
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                postMeter(entry.getKey(), entry.getValue());
            }
        }

    }

    private void postMeter(String key, Meter meter) {
    	if(key.compareToIgnoreCase("uk.ac.imperial.lsds.seepworker.core.SingleThreadProcessingEngine.event.per.sec") == 0){
    		try {
				sendPost("throughput", Double.toString(convertRate(meter.getOneMinuteRate())));
			} catch (Exception e) {
				e.printStackTrace();
			}
    	}   	
//    	  output.printf(locale, "             count = %d%n", meter.getCount());
//        output.printf(locale, "         mean rate = %2.2f events/%s%n", convertRate(meter.getMeanRate()), getRateUnit());
//        output.printf(locale, "     1-minute rate = %2.2f events/%s%n", convertRate(meter.getOneMinuteRate()), getRateUnit());
//        output.printf(locale, "     5-minute rate = %2.2f events/%s%n", convertRate(meter.getFiveMinuteRate()), getRateUnit());
//        output.printf(locale, "    15-minute rate = %2.2f events/%s%n", convertRate(meter.getFifteenMinuteRate()), getRateUnit());
    }

	// HTTP POST request
	private void sendPost(String mode, String value) throws Exception {
		String url = "";
		if( lo.getSeepTask() instanceof Sink)
			url = "http://127.0.0.1:8888/rest/metrics/snk"+lo.getOperatorId();
		else if( lo.getSeepTask() instanceof Source)
			url = "http://127.0.0.1:8888/rest/metrics/src"+lo.getOperatorId();
		else
			url = "http://127.0.0.1:8888/rest/metrics/op"+lo.getOperatorId();
		
		URL obj = new URL(url);
		HttpURLConnection con =  (HttpURLConnection) obj.openConnection();

		//add request header
		con.setRequestMethod("POST");
		String urlParameters = "mode="+ mode +"&value="+ value;
		
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		if(responseCode == 200)
			return;

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		
		//print result
		System.err.println("ERROR Responce: "+ response.toString());

	}
}


