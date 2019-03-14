package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UnknownPicsToBin extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new UnknownPicsToBin(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		//Path inPath=new Path(conf.get("inpath"));
		Path outPath=new Path("/knn_data/train_bin1");
		
		File file = new File("./train");		//获取其file对象
		File[] fs = file.listFiles();	//遍历path下的文件和目录，放在File数组中
		for(File f:fs){					//遍历File[]数组
			if(!f.isDirectory())		//若非目录(即文件)，则打印
				putToHdfs(new Path(f.toString()), outPath, conf);
		}

		
		
		// TODO 自动生成的方法存根
		return 0;
	}
	
	public static void putToHdfs(Path inPath,Path outpath,Configuration conf) throws IOException {
		//本地文件系统
		LocalFileSystem local=FileSystem.getLocal(conf);
		//hdfs文件系统
		FileSystem fs=FileSystem.get(conf);
		
		FSDataInputStream in = local.open(inPath);
		
		
		
		if (fs.exists(outpath)) {
			FSDataOutputStream out = fs.append(outpath);
			System.out.println("inpath:"+inPath);
			String label=new String(inPath.toString().split("/")[1].split("\\.")[0]);
			
			//二值化过程
			
			picToBin(in, out, true,label);
		}
		else {
			FSDataOutputStream out = fs.create(outpath);
			System.out.println("inpath:"+inPath);
			String label=new String(inPath.toString().split("/")[1].split("\\.")[0]);
			
			//二值化过程
			
			picToBin(in, out, true,label);
		}

		
	}
	
	public static void picToBin(InputStream in,OutputStream out,boolean close,String label) throws FileNotFoundException, IOException {
		//BufferedImage
		
		BufferedImage img=ImageIO.read(in);
		//out没有输出字符的方法
		PrintWriter write=new PrintWriter(out);
		
		int height=img.getHeight();
		int width=img.getWidth();

		write.write(label);
		write.flush();
		write.write(",");
		write.flush();
		for(int i=0;i<height;i++) {
			for(int j=0;j<width;j++) {
				int rgb = img.getRGB(j, i);
				Color gray=new Color(127, 127, 127);
				
				int g_rgb=gray.getRGB();
				if (rgb<g_rgb) {
					System.out.print("0");
					write.write("0");
					write.flush();
				}
				else {
					System.out.print("1");
					write.write("1");
					write.flush();
				}
			}
			System.out.println();
		}//外for
		
		write.write("\n");
		write.flush();
		
		if (close) {
			in.close();
			write.close();
		}
	}
}
