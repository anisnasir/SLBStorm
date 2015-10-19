/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfGeneratorSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	int numMessages;
	int k; //unique Elements
	double skew;
	ZipfDistribution zipf;
	String randomStr;
	int messageCount;

	public String fillString(int count,char c) {
	    StringBuilder sb = new StringBuilder( count );
	    for( int i=0; i<count; i++ ) {
	        sb.append( c ); 
	    }
	    return sb.toString();
	}

	private static String createDataSize(int msgSize) {
		StringBuilder sb = new StringBuilder(msgSize);
		for (int i=0; i<msgSize; i++) {
			sb.append('a');
		}
		return sb.toString();
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		numMessages = 41666;
		k = 10000;
		skew = 2.0;
		zipf = new ZipfDistribution(k,skew);
		randomStr = createDataSize(500);
		messageCount = 0;

	}
	@Override
	public void nextTuple() {
		if(messageCount < numMessages ) {
			long num = zipf.sample();
			long timeStamp = System.currentTimeMillis();
			String sentence = String.valueOf(num);
			_collector.emit(new Values(sentence,timeStamp),num);
			messageCount++;	
		}
		return;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

}