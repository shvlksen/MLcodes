
//authored by keed

//association analysis using market basket case

package newword3;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class frequent3 extends Configured implements Tool
{
  
	// public static int minsup;
 	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final Text itemList = new Text();
		private static final IntWritable countItemList = new IntWritable(1);
		public static List selfCross(List inputList, int pairOfItems)
    		{
			List retItems = null;
			List items = null;
			List tmpItems = null;
			int index = 0;
			for(Iterator iterator = inputList.iterator(); iterator.hasNext();)
			{
			    String item = (String)iterator.next();
			    if(retItems == null)
				retItems = new LinkedList();
			    for(int i = index + 1; i < inputList.size(); i++)
			    {
				items = new LinkedList();
				items.add(item);
				retItems.addAll(selfCrossList(items, inputList, i, pairOfItems));
			    }

			    index++;
			}

			return retItems;
    		}
		protected static List selfCrossList(List currList, List inputList, int nextLoc, int recrv)
		    {
			List ret = new LinkedList();
			if(nextLoc < inputList.size())
			{
			    currList.add((String)inputList.get(nextLoc));
			    if(recrv == 2)
			    {
				ret.add(currList);
			    } else
			    {
				for(int i = nextLoc + 1; i < inputList.size(); i++)
				{
				    List tmpList = (LinkedList)((LinkedList)currList).clone();
				    ret.addAll(selfCrossList(tmpList, inputList, i, recrv - 1));
				}

			    }
			}
			return ret;
		    }
		
		public void map(LongWritable key1, Text value1,  OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{		
			int no_pairs=3;
			String line = value1.toString();
			StringTokenizer itr = new StringTokenizer(line);

			List<String> itemsST = new LinkedList<String>();			
			while (itr.hasMoreTokens())
			{ 
				itemsST.add((String)itr.nextToken());			
			}
			Collections.sort(itemsST);
			
			Iterator it = itemsST.iterator();
			List<String> items = new LinkedList<String>();	
			while (it.hasNext())
			{ 
				items.add((String)it.next());	
				List<List<String>> outItems = null;
				String itemPair = null;
				for(int i=0;i<items.size();i++)
				{ 
					outItems =selfCross(items, no_pairs);
					for(List<String> lst: outItems)
					{
						itemPair = lst.get(0);
						for(int j=1;j<no_pairs;j++)
						{
							itemPair += (" " + lst.get(j)); 
						}
						itemList.set(itemPair);
						output.collect(itemList, countItemList);
					}
					
					
				}
			}
		}		
	}
	
	
	
	 public static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	 {
		private static final IntWritable totalItemsPaired = new IntWritable(0);
		public void reduce(Text key, Iterator<IntWritable> value2, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{
			int sum = 0;
			while(value2.hasNext())
			{
				sum += value2.next().get();
			}
			totalItemsPaired.set(sum);
			//if(sum>3000)
			output.collect(key, totalItemsPaired);
		}
	}
 
  
  

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), frequent3.class);
    	conf.setJobName("frequent3");
    	conf.setOutputKeyClass(Text.class);
    	conf.setOutputValueClass(IntWritable.class);
    	conf.setMapperClass(MyMapper.class);        
    	conf.setCombinerClass(MyReducer.class);
    	conf.setReducerClass(MyReducer.class);
	conf.setOutputFormat(TextOutputFormat.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setNumReduceTasks(2);  
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
	//minsup=Integer.parseInt(args[2]);
    int res = ToolRunner.run(new Configuration(), new frequent3(), args);
    System.exit(res);
  }

}
