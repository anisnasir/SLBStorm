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


public class DChoiceGrouping implements CustomStreamGrouping,Serializable{
	private static final long serialVersionUID = 1L;
	private List<Integer> targetTasks;
	private  long[] targetTaskStats;
	WorkerTopologyContext context;
	private HashFunction h1 = Hashing.murmur3_128(13);
	private HashFunction h2 = Hashing.murmur3_128(17);
	private HashFunction[] hash;
	StreamSummary<String> streamSummary;
	int serversNo;
	Long totalItems;
	private Seed seeds;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		this.context = context;
		this.targetTasks = targetTasks;
		targetTaskStats = new long[this.targetTasks.size()];
		streamSummary = new StreamSummary<String>(StreamSummaryHelper.capacity);
		totalItems = (long) 0;
		serversNo = this.targetTasks.size();
		seeds = new Seed(serversNo);
		hash = new HashFunction[this.serversNo];
		for (int i=0;i<hash.length;i++) {
			hash[i] = Hashing.murmur3_128(seeds.SEEDS[i]);
		}
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
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
				double epsilon = 0.0001;
				int Choice =2;
				HashMap<String,Long> freqList = ssHelper.getTopK(streamSummary,probability,totalItems);
				if(freqList.containsKey(str)) {
					double pTop = ssHelper.getPTop(streamSummary,this.totalItems);
					PHeadCount pHead = ssHelper.getPHead(streamSummary,probability,this.totalItems);
					double pTail = 1-pHead.probability;
					double n = (double)this.serversNo;
					double val1 = (n-1)/n;
					double d = FastMath.round(pTop*this.serversNo);
					double val2,val3,val4,sum1;
					double sum2,value1,value2,value3,value4;
					do{
						//finding sum Head
						val2 = FastMath.pow(val1, pHead.numberOfElements*d);
						val3 = 1-val2;
						val4 = FastMath.pow(val3, 2);
						sum1 = pHead.probability + pTail*val4;

						//finding sum1
						value1 = FastMath.pow(val1, d);
						value2 = 1-value1;
						value3 = FastMath.pow(value2, d);
						value4 = FastMath.pow(value2, 2);
						sum2 = pTop+((pHead.probability-pTop)*value3)+(pTail*value4);
						d++;
					}while((d<=this.serversNo) && ((sum1 > (val3+epsilon)) || (sum2 > (value2+epsilon))));			
					Choice = (int)d-1;

					//Hash the key accordingly
					int counter = 0;
					int[] choice;
					byte[] b = str.toString().getBytes();
					if(Choice < this.serversNo) {
						choice = new int[Choice];
						while(counter < Choice) {
							choice[counter] =  FastMath.abs(hash[counter].hashBytes(b).asInt()%serversNo); 
							counter++;
						}
					}else {
						choice = new int[this.serversNo];
						while(counter < this.serversNo) {
							choice[counter] =  counter; 
							counter++;
						}
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
