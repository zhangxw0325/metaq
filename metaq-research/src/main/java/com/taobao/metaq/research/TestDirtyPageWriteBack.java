/**
 * $Id: TestDirtyPageWriteBack.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.research;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.taobao.metaq.store.MapedFile;

public class TestDirtyPageWriteBack
{
	public static final int _1K=1024;
	public static final int _1G=_1K*_1K*_1K;
	public static byte[] DATA;
	public static long max=0;
	public static int count=0;
	public static int count_10_50=0;		// 1
	public static int count_50_100=0;		// 2
	public static int count_100_500=0;		// 3
	public static int count_500_1000=0;		// 4
	public static int count_1000_=0;		// 5
	public static int total=0;
	
	public static int CUT_SIZE=1000*1000*1000;
	public static int LIST_SIZE=10*1000*1000;
	public static List<SoftReference<MapedFile>> list=new ArrayList<SoftReference<MapedFile>>();
	static class Daemon extends Thread
	{
		public Daemon(String name)
		{
			super(name);
		}
		
		public void run()
		{
			try
			{
				while(true)
				{
					Thread.sleep(3000);
					int c=list.size();
					if(c>CUT_SIZE)
					{
						list=new ArrayList<SoftReference<MapedFile>>(list.subList(list.size()-LIST_SIZE, c));
					}
					
					SoftReference<MapedFile>[] array=list.toArray(new SoftReference[]{});
					for(SoftReference<MapedFile> sr:array)
					{
						MapedFile mp=sr.get();
						if(mp!=null)
							mp.commit(0);
					}
				}
			}
			catch(Throwable e)
			{
				System.out.println("list size"+list.size());
				e.printStackTrace();
			}
		}
	}
	
	static
	{
		int i=4*_1K;
		StringBuilder sb=new StringBuilder();
		while(i-->0)
		{
			sb.append("k");
		}
		DATA=sb.toString().getBytes();
	}
	
	/*
	 * vm.dirty_ratio
	 * vm.min_free_kbytes
	 * vm.dirty_background_ratio *
	 */
	public static void main(String[] args) throws Exception
	{
		try
		{
			int index=0;
			String pre="/home/admin/test/0000000";
			MapedFile mp=new MapedFile(pre+(++index),_1G);
			list.add(new SoftReference<MapedFile>(mp));
	
			new Daemon("daemon-commit").start();
			
			int len=-1;
			long temp=-1;
			while(true)
			{
				long start=System.currentTimeMillis();
				//len=mp.appendMessage(DATA);
				long end=System.currentTimeMillis();
				total++;
				if((temp=end-start)>10)
				{
					if(max<temp)
					{
						max=temp;
					}
					
					switch(range(temp))
					{
						case 1:
							count_10_50++;
							break;
						case 2:
							count_50_100++;
							break;
						case 3:
							count_100_500++;
							break;
						case 4:
							count_500_1000++;
							break;
						case 5:
							count_1000_++;
							break;
						default:
							assert false;
					}		
					count++;
					System.out.println("total:"+total+",block:"+count+",cost:"+(end-start)+",start:"+start+",end:"+end+",end:"+new Date(end));
					System.out.println("total:"+total+",block:"+count+","+((float)count/(float)total)+";[10,50):"+count_10_50+","
							+((float)count_10_50/(float)total)+";[50,100):"+count_50_100+","+((float)count_50_100/(float)total)+";[100,500):"
							+count_100_500+","+((float)count_100_500/(float)total)+";[500,1000)"+count_500_1000+","+((float)count_500_1000/(float)total)
							+";[1000,+00):"+count_1000_+","+((float)count_1000_/(float)total)+",max:"+max);
				}
				if(-1==len)
				{
					mp=new MapedFile(pre+(++index),_1G);
					list.add(new SoftReference<MapedFile>(mp));
				}
			}
		}
		catch(Throwable e)
		{
			System.out.println("list size"+list.size()+" ----------");
			e.printStackTrace();
		}
	}

	public static int range(long t)
	{
		int ret=-1;
		if(t/10>=1 && t/10<5)
		{
			ret=1;
		}
		else if(t/10>=5 && t/10<10)
		{
			ret=2;
		}
		else if(t/10>=10 && t/10<50)
		{
			ret=3;
		}
		else if(t/10>=50 && t/10<100)
		{
			ret=4;
		}
		else if(t/10>=100 )
		{
			ret=5;
		}
		
		return ret;
	}
}
