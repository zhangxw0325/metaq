package com.taobao.metamorphosis.tools.fresh;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.text.MessageFormat;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.taobao.metaq.commons.MetaUtil;

public class ParseMessage10 
{
	public static final String TITLE="Original size	Zip size	Ratio	Gzip size	Ratio	Original compress	Ratio";
	public static final MessageFormat format=new MessageFormat("{0}		{1}		{2}	{3}		{4}	{5}			{6}");
	
	/**
	 * 解析1.4版本的meta server消息
	 * 1.4 消息格式：
	 * |------4-----|-----4-----|---8---|---4---|--data length--|
	 * |data length	|checksum	|msgId	|flag	|data			|
	 * 
	 * @param
	 * args[0]:file文件路径
	 */
	public static void main(String[] args) throws Exception
	{
		if(args==null || args.length<1)
		{
			System.err.println("input file path.");
			System.exit(0);
		}
		long start=System.currentTimeMillis();
		String filename=args[0];
		File file=new File(filename);
		RandomAccessFile raf=new RandomAccessFile(file,"r");
		FileChannel fc=raf.getChannel();
		MappedByteBuffer mbb=fc.map(MapMode.READ_ONLY, 0, file.length());
		System.out.println(TITLE);
		try
		{
			while(mbb.hasRemaining())
			{
				byte[] data=parseMessage(mbb);
				byte[] odata=originalCompress(data);
				byte[] gdata=gzip(data);
				byte[] zdata=zip(data);
				
				System.out.println(format.format(new Object[]{data.length,zdata.length,
						(float)zdata.length/(float)data.length,gdata.length,
						(float)gdata.length/(float)data.length,odata.length,
						(float)odata.length/(float)data.length}));
			}
		}
		finally
		{
			fc.close();
			raf.close();
		}
		System.out.println(System.currentTimeMillis()-start);
	}

	//返回消息内容
	public static byte[] parseMessage(ByteBuffer buffer)
	{
		 int size=buffer.getInt();
		 buffer.getInt();
		 buffer.getLong();
		 int flag=buffer.getInt();
		 if((flag & 0x1)==1)
		 {
			 int attributeLen=buffer.getInt();
			 buffer.position(buffer.position()+attributeLen);
		 }
		 
		 //FIXME bad practise , Buffer.slice maybe better
		 byte[] bytes=new byte[size];
		 buffer.get(bytes, 0, size);
		 
		 return bytes;
	}
	
	public static byte[] zip(byte[] data) throws Exception
	{
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		ZipOutputStream zos=new ZipOutputStream(bos);
		zos.setLevel(9);
		ZipEntry entry = new ZipEntry("./temp");
		zos.putNextEntry(entry);
		zos.write(data);
		zos.closeEntry();
		zos.close();
		
		byte[] r=bos.toByteArray();
		bos.close();
		
		return r;
	}
	
	public static byte[] dzip(byte[] data) throws Exception
	{
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		ByteArrayInputStream bais=new ByteArrayInputStream(data);
		ZipInputStream zis=new ZipInputStream(bais);
		ZipEntry entry=null;
		while((entry=zis.getNextEntry())!=null)
		{
			if(!entry.isDirectory())
			{
				byte[] bytes=new byte[1024];
				int count;
				while((count=zis.read(bytes))!=-1)
				{
					bos.write(bytes, 0, count);
				}
			}
			zis.closeEntry();
		}
		
		byte[] r=bos.toByteArray();
		zis.close();
		bais.close();
		bos.close();
		
		return r;
	}
	
	public static byte[] gzip(byte[] data) throws Exception
	{
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		GZIPOutputStream gzos=new GZIPOutputStream(bos);
		gzos.write(data);
		gzos.finish();
		gzos.flush();
		byte[] r=bos.toByteArray();
		gzos.close();
		bos.close();
		
		return r;
	}
	
	public static byte[] ungzip(byte[] data) throws Exception
	{
		ByteArrayInputStream bais=new ByteArrayInputStream(data);
		GZIPInputStream gzip=new GZIPInputStream(bais);
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		byte[] bytes=new byte[1024];
		int count;
		while((count=gzip.read(bytes))!=-1)
		{
			bos.write(bytes, 0, count);
		}
		
		byte[] r=bos.toByteArray();
		bos.close();
		gzip.close();
		bais.close();
		
		return r;
	}
	
	public static byte[] originalCompress(byte[] data) throws Exception
	{
		return MetaUtil.compress(data, 9);
	}
	
	public static byte[] originalDecompress(byte[] data) throws Exception
	{
		return MetaUtil.uncompress(data);
	}
}
