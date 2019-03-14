package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainPicToBin extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrainPicToBin(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf=getConf();
		Path inpath=new Path("/knn_data/train");
		Path outpath=new Path("/knn_data/train_bin");
		
		allPicsToBin(inpath, outpath, conf);
		return 0;
	}
	
	public static void allPicsToBin(Path inpath,Path outpath,Configuration conf) throws IOException {
		//inpath,hdfs中的目录
		FileSystem fS=FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> files = fS.listFiles(inpath, true);
		//读取每个文件内容，二值化，输出到seqfile
		//选项1 seqfile输出路径
		SequenceFile.Writer.Option option1=SequenceFile.Writer.file(outpath);
		// key文件前缀名
		SequenceFile.Writer.Option option2=SequenceFile.Writer.keyClass(Text.class);
		//value 二值化向量
		SequenceFile.Writer.Option option3=SequenceFile.Writer.valueClass(Text.class);
		
		//seqfile 输出流
		SequenceFile.Writer writer=SequenceFile.createWriter(conf, option1, option2, option3);
		//用来接收k和v的值
		Text k=new Text();
		Text v=new Text();
		while (files.hasNext()) {
			//二值化每个图片，把二值化结果，设置为v，
			LocatedFileStatus file = files.next();
			String name = file.getPath().getName();
			String new_name = name.substring(0, name.indexOf("."));
			k.set(new_name);
			
			FSDataInputStream in = fS.open(file.getPath());
			//调用二值化方法
			String bin_line=picTobin(in);
			v.set(bin_line);
			writer.append(k, v);
			
		}
		writer.close();
	}

	private static String picTobin(FSDataInputStream in) throws IOException {
		//BufferedImage
		
		BufferedImage img=ImageIO.read(in);
		//out没有输出字符的方法		
		
		StringBuffer sb=new StringBuffer();
		
		int height=img.getHeight();
		int width=img.getWidth();

		
		for(int i=0;i<height;i++) {
			for(int j=0;j<width;j++) {
				int rgb = img.getRGB(j, i);
				Color gray=new Color(127, 127, 127);
				
				int g_rgb=gray.getRGB();
				if (rgb<g_rgb) {
					System.out.print("0");
					sb.append("0");
				}
				else {
					System.out.print("1");
					sb.append("1");
				}
			}
			System.out.println();
		}//外for
		
		return sb.toString();
	}
	
	
}
