package com.briup.knn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TagAvgNum implements Writable{
	
	private Text tag=new Text();
	private DoubleWritable avg=new DoubleWritable();
	private IntWritable num=new IntWritable();
	
	public TagAvgNum() {
		// TODO 自动生成的构造函数存根
	}
	
	public TagAvgNum(Text line) {
		// TODO 自动生成的构造函数存根
		String[] infos = line.toString().split("\t");
		this.tag=new Text(infos[0]);
		this.avg=new DoubleWritable(Double.parseDouble(infos[1]));
		this.num=new IntWritable(Integer.parseInt(infos[2]));
	}
	
	public TagAvgNum(String tag,double avg,int num) {
		// TODO 自动生成的构造函数存根
		this.tag=new Text(tag);
		this.avg=new DoubleWritable(avg);
		this.num=new IntWritable(num);
	}
	
	public TagAvgNum(TagAvgNum tan) {
		// TODO 自动生成的构造函数存根
		this.tag=tan.getTag();
		this.avg=tan.getAvg();
		this.num=tan.getNum();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO 自动生成的方法存根
		this.tag.readFields(in);
		this.avg.readFields(in);
		this.num.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO 自动生成的方法存根
		this.tag.write(out);
		this.avg.write(out);
		this.num.write(out);
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = new Text(tag.toString());
	}

	public DoubleWritable getAvg() {
		return avg;
	}

	public void setAvg(DoubleWritable avg) {
		this.avg = new DoubleWritable(avg.get());
	}

	public IntWritable getNum() {
		return num;
	}

	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}
	
}
