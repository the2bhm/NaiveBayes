package bayes;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NaiveBayesMapper extends MapReduceBase implements Mapper<LongWritable, Text,Text,DoubleWritable>{
   
	String delimiter,continousVariables,discreteVariables;	
	int targetVariable,numColums;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	
	public HashSet<Integer> splitvariables(String varString){
		HashSet<Integer> hs = new HashSet<Integer>();
		//构造解析varString的对象，并指定分隔符为','.
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(Integer.parseInt(tok.nextToken()));
		return hs;
	}
	
	//传入数据
	@Override
	 public void configure(JobConf conf){
		//分隔符
		delimiter = conf.get("delimiter");
		numColums = conf.getInt("numColumns", 0);
		continousVariables = conf.get("continousVariables");
		discreteVariables = conf.get("discreteVariables");
	    targetVariable = conf.getInt("targetVariable",0);
	    discreteVariablesIndex = new HashSet<Integer>();
		continousVariablesIndex = new HashSet<Integer>();
		//分隔传入变量的数据
	    if(continousVariables!=null)
	    continousVariablesIndex = splitvariables(continousVariables);
	    if(discreteVariables!=null)
	    discreteVariablesIndex = splitvariables(discreteVariables);
	}
	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<Text, DoubleWritable> output, Reporter arg3)
			throws IOException {
		Integer varIndex = 1; 
		String record = value.toString();
		//分隔数据集中的数据
		String features[] = record.split(delimiter);
	    for(int i = 0 ;i < numColums ; i++){//数据集的行数
	    	if(varIndex!= targetVariable){
	    		if(discreteVariablesIndex.contains(varIndex))
	    		 output.collect(new Text(varIndex+"_"+features[i]+"_"+features[targetVariable-1]), new DoubleWritable(1.0));
	    		if(continousVariablesIndex.contains(varIndex))
	    		 output.collect(new Text(varIndex+"_"+features[targetVariable-1]), new DoubleWritable(Double.parseDouble(features[i])));
	    	}
	    	varIndex ++;
	}
	    output.collect(new Text(targetVariable+"_"+features[targetVariable-1]), new DoubleWritable(1.0));
	    output.collect(new Text(targetVariable+""), new DoubleWritable(1.0));
	}
}
