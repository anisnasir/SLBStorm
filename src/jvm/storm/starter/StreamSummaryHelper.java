package storm.starter;

import java.util.HashMap;
import java.util.List;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;



public class StreamSummaryHelper {
	static int capacity = 100;
	public HashMap<String,Long> getTopK(StreamSummary<String> topk, float probability, Long totalItems) {
		HashMap<String,Long> returnList = new HashMap<String,Long>();
		List<Counter<String>> counters = topk.topK(topk.getCapacity()); 

		for(Counter<String> counter : counters)  {
			float freq = counter.getCount();
			float error = counter.getError();
			float itemProb = (freq+error)/totalItems;
			if (itemProb > probability) {
				returnList.put(counter.getItem(),counter.getCount());
			}
		}
		return returnList;

	}
	public PHeadCount getPHead(StreamSummary<String> topk, double probability, Long totalItems) {
		PHeadCount returnValue = new PHeadCount();
		returnValue.probability=0;

		List<Counter<String>> counters = topk.topK(topk.getCapacity()); 

		for(Counter<String> counter : counters)  {
			float freq = counter.getCount();
			float error = counter.getError();
			float itemProb = (freq+error)/totalItems;
			if (itemProb > probability) {
				returnValue.probability+=itemProb;
				returnValue.numberOfElements++;
			}
		}
		return returnValue;	
	}
	public float getPTop(StreamSummary<String> topk, Long totalItems) {
		List<Counter<String>> counters = topk.topK(1);
		for(Counter<String> counter : counters)  {
			float freq = counter.getCount();
			float error = counter.getError();
			float itemProb = (freq+error)/totalItems;
			return itemProb;
		}
		return 0f;
	}
}
