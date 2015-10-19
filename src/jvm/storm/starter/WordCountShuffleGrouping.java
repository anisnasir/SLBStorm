package storm.starter;

import java.util.HashMap;
import java.util.Map;
import java.util.Locale;
import java.util.Random;

import backtype.storm.Constants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import storm.starter.spout.ZipfGeneratorSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class WordCountShuffleGrouping {
	public static class CounterBolt extends BaseRichBolt {
		private static final String GRAPHITE_METRICS_NAMESPACE_PREFIX = "zipf_26_26_SG";
		private static final Pattern hostnamePattern =
				Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");
		int boltId = -1;

		private transient Meter tuplesReceived;
		private transient Meter memoryUsed;
		private transient Histogram histogram;

		private static final long serialVersionUID = 1L;
		OutputCollector collector;

		

		private void initializeMetricReporting()
		{
			final MetricRegistry registry = new MetricRegistry();
			final CsvReporter reporter = CsvReporter.forRegistry(registry)
					.formatFor(Locale.US)
					.convertRatesTo(TimeUnit.MINUTES)
					.build(new File("/mnt/anis-logs/"));
			reporter.start(1,TimeUnit.MINUTES);
			tuplesReceived = registry.meter(MetricRegistry.name("tuples", "received",metricsPath()));
			memoryUsed = registry.meter(MetricRegistry.name("memory","received",metricsPath()));
			histogram =  registry.histogram(MetricRegistry.name("histogram","time",metricsPath()));

		}


		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","count"));
		}

		private String metricsPath() {
			if (boltId == -1) {
				Random rnd = new Random();
				boltId = rnd.nextInt(1000);
			}
			final String myHostname = extractHostnameFromFQHN(detectHostname());

			return GRAPHITE_METRICS_NAMESPACE_PREFIX + "." + myHostname+"."+boltId;
		}


		private static String detectHostname()
		{
			String hostname = "hostname-could-not-be-detected";
			try {
				hostname = InetAddress.getLocalHost().getHostName();
			}catch (UnknownHostException e) {
			}
			return hostname;
		}

		private static String extractHostnameFromFQHN(String fqhn)
		{
			if (hostnamePattern.matcher(fqhn).matches())
			{
				if (fqhn.contains("."))
				{
					return fqhn.split("\\.")[0];
				}
				else
				{
					return fqhn;
				}
			}
			else {
				return fqhn;
			}         
		}

		@Override
		public void execute(Tuple tuple) {
			String word = tuple.getStringByField("word");
			Long startTime = tuple.getLongByField("count");
			
			int count = 0;
			if(!word.isEmpty())
			{
				testWait(400000);
				if(counts.containsKey(word)) {
					count = counts.get(word);
					count++;
					counts.put(word, count);
				}else {
					counts.put(word, 1);
					count = 1;
					memoryUsed.mark();	
				}
					
				tuplesReceived.mark();
				collector.emit(new Values(word, count));

			}
			
			collector.ack(tuple);
			Long endTime = System.currentTimeMillis();
			Long executionLatency = endTime-startTime;
			histogram.update(executionLatency);
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			initializeMetricReporting();
			collector = arg2;
			
		}
		public void testWait(long INTERVAL){
		    long start = System.nanoTime();
		    long end=0;
		    do{
		        end = System.nanoTime();
		    }while(start + INTERVAL >= end);
		    System.out.println(end - start);
		}
	}
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stream", new ZipfGeneratorSpout(), 26);
		builder.setBolt("counter", new CounterBolt(),26).shuffleGrouping("stream");
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxSpoutPending(100);
		//conf.setMessageTimeoutSecs(300);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(52);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(14400000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
