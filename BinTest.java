package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.imageio.ImageIO;


public class BinTest {
	public static void main(String[] args) throws FileNotFoundException, IOException {
		//BufferedImage
		
		BufferedImage img=ImageIO.read(new FileInputStream("src/0_13.png"));
		
		int height=img.getHeight();
		int width=img.getWidth();
		
		for(int i=0;i<height;i++) {
			for(int j=0;j<width;j++) {
				int rgb = img.getRGB(j, i);
				Color gray=new Color(127, 127, 127);
				
				int g_rgb=gray.getRGB();
				if (rgb<g_rgb) {
					System.out.print("0");
				}
				else {
					System.out.print("1");
				}
			}
			System.out.println();
		}
	}
}
