package com.briup.knn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByDegree extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SortByDegree(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "sbd");
		job.setJarByClass(this.getClass());
		job.setMapperClass(SBDMapper.class);
		job.setMapOutputKeyClass(TagDegree.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path("/knn_data/result/part-r-00000"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/result_sorted"));
		job.setGroupingComparatorClass(TagDegreeGroupComparator.class);
		job.waitForCompletion(true);
		return 0;
	}

	public static class SBDMapper extends Mapper<LongWritable, Text, TagDegree, NullWritable> {
		// 整理复合键
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, TagDegree, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] infos = line.split("\t");
			String tag = infos[0];
			String degree = infos[1];
			TagDegree tg = new TagDegree(tag, Double.parseDouble(degree));
			context.write(tg, NullWritable.get());
		}
	}
}
