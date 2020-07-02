package bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NaiveBayesTrainJob extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		//创建对象
		JobConf conf = new JobConf(getConf(),NaiveBayesTrainJob.class);
		conf.setJobName("Training");
		//设置map端输出的数据类型
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		//设置输出的数据类型
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		//设置mapper和reduce处理的任务数
		conf.setNumMapTasks(conf.getInt("numMaps", 4));
		conf.setNumReduceTasks(conf.getInt("numReduce", 1));
		//设置mapper类
		conf.setMapperClass(NaiveBayesMapper.class);
		//设置reducer类
		conf.setReducerClass(NaiveBayesReducer.class);
		//数据集输入
		FileInputFormat.addInputPath(conf, new Path(conf.get("input")));
		//结果输出
		FileOutputFormat.setOutputPath(conf, new Path(conf.get("output")));
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NaiveBayesTrainJob(), args);
		System.exit(res);
	}
}
