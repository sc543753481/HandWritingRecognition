package com.briup.knn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import test.knn.TagAvgNum;


public class GetLastResult extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetLastResult(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"getLastResult");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(GLRMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TagAvgNum.class);
		
		job.setReducerClass(GLRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//为job指定输入路径
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job,new Path("/knn_data/result_top20/part-r-00000"));
		//为job指定输出路径
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/last_result"));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	//map整理数据 
	//reduce把一行数据想办法看成一个整体
	//可以是数组，可以是键值对，可以是自定义类型
	// map整理数据 整理自定义类型
	public static class GLRMapper extends Mapper<LongWritable, Text, Text, TagAvgNum> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TagAvgNum>.Context context)
				throws IOException, InterruptedException {
			TagAvgNum tan = new TagAvgNum(value);
			context.write(new Text("a"), tan);
		}
	}

	// reduce 把一行数据想办法看成一个整体
	// 可以是数组，可以是键值对，可以是自定义类型
	public static class GLRReducer extends Reducer<Text, TagAvgNum, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<TagAvgNum> values,
				Reducer<Text, TagAvgNum, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			Iterator<TagAvgNum> ite = values.iterator();
			TagAvgNum max = new TagAvgNum(ite.next());
			while (ite.hasNext()) {
				TagAvgNum current = new TagAvgNum(ite.next());
				if (max.getNum().get() < current.getNum().get()) {
					max = new TagAvgNum(current);
				} else if (max.getNum().get() == current.getNum().get()) {
					if (max.getAvg().get() < current.getAvg().get()) {
						max = new TagAvgNum(current);
					}
				}
			}
			context.write(max.getTag(), NullWritable.get());
		}
	}
}
