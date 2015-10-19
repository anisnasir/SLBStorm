package storm.starter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.util.FastMath;

import com.clearspring.analytics.stream.StreamSummary;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;


public class WChoiceGrouping implements CustomStreamGrouping,Serializable{
	private static final long serialVersionUID = 1L;
	private List<Integer> targetTasks;
	private  long[] targetTaskStats;
	WorkerTopologyContext context;
	private HashFunction h1 = Hashing.murmur3_128(13);
	private HashFunction h2 = Hashing.murmur3_128(17);
	StreamSummary<String> streamSummary;
	Long totalItems;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		this.context = context;
		this.targetTasks = targetTasks;
		targetTaskStats = new long[this.targetTasks.size()];
		streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
		totalItems = (long) 0;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		List<Integer> boltIds = new ArrayList<Integer>();
		StreamSummaryHelper ssHelper = new StreamSummaryHelper();
		if(values.size()>0)
		{
			String str = values.get(0).toString();
			Long timeStamp = Long.parseLong(values.get(1).toString());
			
			if(str.isEmpty())
				boltIds.add(targetTasks.get(0));
			else
			{
				streamSummary.offer(str);
				float probability = 2/(float)(this.targetTasks.size()*10);
				HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);
				if(freqList.containsKey(str)) {
					int choice[] = new int[this.targetTasks.size()];
					int count = 0;
					while(count < this.targetTasks.size()) {
						choice[count] = count;
						count++;
					}
					int selected = selectMinChoice(targetTaskStats,choice);
					boltIds.add(targetTasks.get(selected));
					targetTaskStats[selected]++;

				}else {

					int firstChoice = (int) (FastMath.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
					int secondChoice = (int) (FastMath.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size());
					int selected = targetTaskStats[firstChoice]>targetTaskStats[secondChoice]?secondChoice:firstChoice;

					boltIds.add(targetTasks.get(selected));
					targetTaskStats[selected]++;
				}
			}		
		}
		return boltIds;
	}
	int selectMinChoice(long loadVector[], int choice[]) {
		int index = choice[0];
		for(int i = 0; i< choice.length; i++) {
			if (loadVector[choice[i]]<loadVector[index])
				index = choice[i];
		}
		return index;
	}

}
