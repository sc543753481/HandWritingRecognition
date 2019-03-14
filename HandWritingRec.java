package com.briup.knn;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.briup.knn.AverageSimilarity.ASMapper;
import com.briup.knn.AverageSimilarity.ASReducer;
import com.briup.knn.CalculationSimilarity.CSMapper;
import com.briup.knn.GetLastResult.GLRMapper;
import com.briup.knn.GetLastResult.GLRReducer;
import com.briup.knn.SortByDegree.SBDMapper;

import test.knn.TagAvgNum;
import test.knn.UnkownPicToBin;

public class HandWritingRec extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new HandWritingRec(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf=getConf();
		//清理上一次运行的目录文件
		
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path("/knn_data/unkown"),true);
		fs.delete(new Path("/knn_data/result1"),true);
		fs.delete(new Path("/knn_data/result_sorted"),true);
		fs.delete(new Path("/knn_data/result_top20"),true);
		fs.delete(new Path("/knn_data/last_result"),true);
		//1 待识别图片，二值化并上传到HDFS
		Path inpath=new Path("./unkown.png");
		Path outpath=new Path("/knn_data/unkown");
		UnkownPicToBin.putToHdfs(inpath, outpath, conf);
		//2 计算相似度
		Job gs_job = Job.getInstance(conf,"calculationSimilarity");
		gs_job.setJarByClass(this.getClass());
		
		gs_job.setMapperClass(CSMapper.class);
		gs_job.setMapOutputKeyClass(Text.class);
		gs_job.setMapOutputValueClass(DoubleWritable.class);
		
		//为job指定输入路径
		gs_job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(gs_job,new Path("/knn_data/train_bin1"));
		//为job指定输出路径
		gs_job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(gs_job, new Path("/knn_data/result1"));
		
		
		//3 按照相似度进行排序
		Job sbd_job = Job.getInstance(conf, "sbd");
		sbd_job.setJarByClass(this.getClass());
		
		sbd_job.setMapperClass(SBDMapper.class);
		sbd_job.setMapOutputKeyClass(TagDegree.class);
		sbd_job.setMapOutputValueClass(NullWritable.class);
		
		sbd_job.setInputFormatClass(TextInputFormat.class);
		sbd_job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(sbd_job, new Path("/knn_data/result1/part-r-00000"));
		TextOutputFormat.setOutputPath(sbd_job, new Path("/knn_data/result_sorted"));
		
//		4 knn统计平均相似度和标签个数
		Job gtk_job = Job.getInstance(conf, "averageSimilarity");
		gtk_job.setJarByClass(this.getClass());
		
		gtk_job.setMapperClass(ASMapper.class);
		gtk_job.setMapOutputKeyClass(TagDegree.class);
		gtk_job.setMapOutputValueClass(Text.class);
		
		gtk_job.setReducerClass(ASReducer.class);
		gtk_job.setOutputKeyClass(Text.class);
		gtk_job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(gtk_job, new Path("/knn_data/result_sorted/part-r-00000"));
		TextOutputFormat.setOutputPath(gtk_job, new Path("/knn_data/result_top20"));
		
		gtk_job.setGroupingComparatorClass(TagDegreeGroupComparator.class);
		
//		5 获得最终结果
		Job glr_job = Job.getInstance(conf,"getLastResult");
		glr_job.setJarByClass(this.getClass());
		
		glr_job.setMapperClass(GLRMapper.class);
		glr_job.setMapOutputKeyClass(Text.class);
		glr_job.setMapOutputValueClass(TagAvgNum.class);
		
		glr_job.setReducerClass(GLRReducer.class);
		glr_job.setOutputKeyClass(Text.class);
		glr_job.setOutputValueClass(NullWritable.class);
		
		//为job指定输入路径
		glr_job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(glr_job,new Path("/knn_data/result_top20/part-r-00000"));
		//为job指定输出路径
		glr_job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(glr_job, new Path("/knn_data/last_result"));
		
		//装配工作流
		ControlledJob gsd_cj=new ControlledJob(gs_job.getConfiguration());
		ControlledJob sbd_cj=new ControlledJob(sbd_job.getConfiguration());
		ControlledJob gtk_cj=new ControlledJob(gtk_job.getConfiguration());
		ControlledJob glr_cj=new ControlledJob(glr_job.getConfiguration());
		
		glr_cj.addDependingJob(gtk_cj);
		gtk_cj.addDependingJob(sbd_cj);
		sbd_cj.addDependingJob(gsd_cj);
		
		JobControl control=new JobControl("handWritingRec");
		control.addJob(glr_cj);
		control.addJob(gtk_cj);
		control.addJob(sbd_cj);
		control.addJob(gsd_cj);
		
		Thread t=new Thread(control);
		t.start();
		while(!control.allFinished()) {
			
		}
		System.out.println("图片识别完毕！");
		
		FSDataInputStream open = fs.open(new Path("/knn_data/last_result/part-r-00000"));
		
		BufferedReader reader=new BufferedReader(new InputStreamReader(open));
		String result = reader.readLine();
		System.out.println("结果为："+result);
		reader.close();
		fs.close();
		
		System.exit(0);
		return 0;
	}
	
	
}
