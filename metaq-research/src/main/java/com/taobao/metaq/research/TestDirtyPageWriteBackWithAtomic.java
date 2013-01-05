/**
 * $Id: TestDirtyPageWriteBackWithAtomic.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.research;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metaq.store.MapedFile;

/*
 * 测试结果和TestDirtyPageWriteBack类似
 */
public class TestDirtyPageWriteBackWithAtomic
{
	public static final int _1K=1024;
	public static final int _1G=_1K*_1K*_1K;
	public static byte[] DATA;
	public static AtomicLong max=new AtomicLong(0);
	public static AtomicInteger count=new AtomicInteger(0);
	public static AtomicInteger count_10_50=new AtomicInteger(-1);		// 1
	public static AtomicInteger count_50_100=new AtomicInteger(-1);		// 2
	public static AtomicInteger count_100_500=new AtomicInteger(-1);	// 3
	public static AtomicInteger count_500_1000=new AtomicInteger(-1);	// 4
	public static AtomicInteger count_1000_=new AtomicInteger(-1);		// 5
	
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
		int index=0;
		String pre="/home/admin/test/0000000";
		MapedFile mp=new MapedFile(pre+(++index),_1G);
		int len=-1;
		long temp=-1;
		while(true)
		{
			long start=System.currentTimeMillis();
			//len=mp.appendMessage(DATA);
			long end=System.currentTimeMillis();
			if((temp=end-start)>10)
			{
				long cur=-1;
				while((cur=max.get())<temp)
				{
					max.compareAndSet(cur, temp);
				}
				
				switch(range(temp))
				{
					case 1:
						count_10_50.incrementAndGet();
						break;
					case 2:
						count_50_100.incrementAndGet();
						break;
					case 3:
						count_100_500.incrementAndGet();
						break;
					case 4:
						count_500_1000.incrementAndGet();
						break;
					case 5:
						count_1000_.incrementAndGet();
						break;
					default:
						assert false;
				}		
				count.incrementAndGet();
				System.out.println("total count:"+count+",cost:"+(end-start)+",start:"+start+",end:"+end+",end:"+new Date(end));
				System.out.println("total count:"+count+",[10,50):"+count_10_50+",[50,100):"+count_50_100+",[100,500):"
						+count_100_500+",[500,1000)"+count_500_1000+",[1000,+00):"+count_1000_+",max:"+max);
			}
			if(-1==len)
			{
				mp=new MapedFile(pre+(++index),_1G);
			}
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
