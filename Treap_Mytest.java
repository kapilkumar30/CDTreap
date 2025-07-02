/* Testing a Concurrent Binary Search Trees with Fat_node */
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;

//import java.util.concurrent.atomic.AtomicInteger;
public class Treap_Mytest{
	private static int RANGE;
	private static int THREADS;
	private static int PER_THREAD;
	private static int TIME;
	private static int FATSIZE;

	//LockBasedJavaHashMap instance;
	ConFatTreap_SHA256 instance;
	long []opCount;
	long totalOps;
	Thread []th;
	//public SortedList head,tail;
	long start;
	int s_Limit,i_Limit;

	public Treap_Mytest(int num_threads, int range, int time, int fat_node_size, int arg1, int arg2)
	{
		//instance=new LockBasedJavaHashMap();
		instance=new ConFatTreap_SHA256(num_threads,fat_node_size);
		THREADS=num_threads;
		RANGE=range;
		TIME=time;
		FATSIZE=fat_node_size;
		th=new Thread[num_threads];
		opCount=new long[num_threads];
		totalOps=0;
		s_Limit=arg1;
		i_Limit=arg2;
	}
	public void prefill() throws Exception{
		for(int i=0;i<1;i++)
		{
			th[i]=new Fill();
		}
		for(int i=0;i<1;i++)
		{
			th[i].start();
		}
		for(int i=0;i<1;i++)
		{
			th[i].join();
		}
	}


class Fill extends Thread
	{
		int PER_THREAD_PREFILL=RANGE/2;//((RANGE/100)*PRE_FILL)/THREADS;
		
		public void run()
		{	
		   
			//Random rd1=new Random();
			//System.out.println(" \nEntered elements: ");
			for(int i=0;i<PER_THREAD_PREFILL;)
			{
				//int val=(int)(ThreadLocalRandom.current().nextGaussian()*RANGE/4)%RANGE/2+RANGE/2;
				int val=ThreadLocalRandom.current().nextInt(RANGE);
				//System.out.print(val+",");
				//int val=i+1;
				instance.insert(val);
				{ i=i+1;}
				
			}
			//instance.display();
		}
	}
	public void testParallel()throws Exception{
		for(int i=0;i<THREADS;i++)
		{
			th[i]=new AllMethods();
		}
		start=System.currentTimeMillis();
		 for(int i=0;i<THREADS;i++)
         {
            th[i].start();
         }
		 for(int i=0;i<THREADS;i++)
         {
            th[i].join();
         }
	}
	class AllMethods extends Thread{
		public void run()
		{
			long count=0;
			
			long end=System.currentTimeMillis();
			int WARMUP_TIME=5000;
           		for(;(end-start)<=WARMUP_TIME;end=System.currentTimeMillis()){
				int val=ThreadLocalRandom.current().nextInt(RANGE);
				instance.contains(val);
				
			}
			
			end=System.currentTimeMillis();	
			for(int i=0;(end-start)<=TIME+5000;i=i+1)
			{

				
				if(i%100==99){  end=System.currentTimeMillis(); }
				int ch=0;
				int chVal=ThreadLocalRandom.current().nextInt(99);
				if(chVal<s_Limit){ ch=0; }
				else if((chVal>=s_Limit)&&(chVal<i_Limit)) ch=1;
				else ch=2;
				int val=ThreadLocalRandom.current().nextInt(RANGE);
				//int val=(int)(ThreadLocalRandom.current().nextGaussian()*RANGE/4)%RANGE/2+RANGE/2;
				switch(ch){

					case 0:{
						//int val=ThreadLocalRandom.current().nextInt(RANGE);
						boolean exits=instance.contains(val);
						
						} break;
					case 1: {
													
						 instance.insert(val);
						
					
					}
						break;
					case 2:{
							 
						
						instance.remove(val); 
						
					}
					break;
					default: break;
				}
				
				count=count+1; 
			}
			opCount[ThreadID.get()]=count;
		
	}
}
	public long totalOperations()
	{
		for(int i=0;i<THREADS;i++)
		{
			totalOps=totalOps+opCount[i];
		}
		return totalOps;
	}
	
	/*public void display()
	{
		System.out.println("\n Height of Treap: "+instance.calculateTreeHeight(instance.root));
	boolean ins2 = instance.isBST();
		System.out.println("\n BST property = "+ins2);
	boolean ins1 = instance.isHeapOrder();
		System.out.print("\n Heap order property = "+ins1);	
	boolean ins3 = instance.isUnique();
		System.out.println("\ncheckUniqueness = "+ins3);
	}*/
	
	public static void main(String[] args){ 
		int num_threads=Integer.parseInt(args[0]);
		int range=Integer.parseInt(args[1]);
		int time=Integer.parseInt(args[2]);
		int fatnode=Integer.parseInt(args[3]);
		int s_Limit=Integer.parseInt(args[4]);
		int i_Limit=Integer.parseInt(args[5]);
		
		Treap_Mytest ob=new Treap_Mytest(num_threads,range,time,fatnode,s_Limit,i_Limit);
		try{ ob.prefill(); }catch(Exception e){ System.out.println(e); }
		
		try{	ob.testParallel(); }catch(Exception e){ System.out.println(e); }
		long total_Operations=ob.totalOperations();
		double throughput=(total_Operations/(1000000.0*time))*1000;// Millions of Operations per second
		System.out.println("\t:num_threads:"+num_threads+"\t:range:"+range+"\t:total_Operations:"+total_Operations+"\t:throughput:"+throughput+"\t:Fatnode-size:"+fatnode+"\t");		
		//ob.display();		
	}
}	
