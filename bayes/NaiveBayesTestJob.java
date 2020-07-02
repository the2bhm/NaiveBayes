package bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class NaiveBayesTestJob extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		//创建对象
		JobConf conf = new JobConf(getConf(),NaiveBayesTestJob.class);
		Job job = new Job(conf, "Multi-view NaiveBayes Training");
		//传入train好的数据
		DistributedCache.addCacheFile(new Path(conf.get("modelPath")+"/part-00000").toUri(), conf);
		//设置mapper类
		conf.setMapperClass(NaiveBayesTestMapper.class);
		//设置输出的数据类型
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);
		//设置mapper和reducer处理的任务数
		conf.setNumMapTasks(conf.getInt("numMaps", 1));
		conf.setNumMapTasks(conf.getInt("numReduce", 1));
		//数据集输入和结果输出
		FileInputFormat.addInputPath(conf, new Path(conf.get("input")));
	    FileOutputFormat.setOutputPath(conf, new Path(conf.get("output")));
	    JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NaiveBayesTestJob(), args);
		System.exit(res);
	}
}
