package Demo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask1 extends Mapper<LongWritable,Text,Text,Text>
{
	private Map<RangeHandler, Integer> tax = new HashMap<RangeHandler, Integer>();
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException 
	{
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
		for (Path SinglePath : files) {
			//if (SinglePath.getName().equals("/user/cloudera/pension_table/part-m-00000")) 
			//{
				BufferedReader reader = new BufferedReader(new FileReader(SinglePath.toString()));
			String line="";
			while((line=reader.readLine())!=null)
			{
				String data[]=line.split(",");
				int min=(int)(Double.parseDouble(data[0]));
				int max=(int)(Double.parseDouble(data[1]));
				int per=Integer.parseInt(data[2]);
				RangeHandler ob=new RangeHandler(min,max);
				tax.put(ob,per);
			//}
			}
			reader.close();
		}
		if (tax.isEmpty()) 
		{
			throw new IOException("Unable To Load Data from Tax table .");
		}
	}

	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException 
			{
		String[] tData=value.toString().split(",");
		String gender=tData[3].trim();
		double tsal=Double.parseDouble(tData[5])*12;
		
		int sal=(int)tsal;
		double taxa=0.0;
		int per=0;
		Set<RangeHandler> keyP=tax.keySet();
		if(tsal>50000)
		{
			for(RangeHandler r:keyP)
			{
				int min=r.getMin();
				int max=r.getMax();
				if((sal>min) && (sal<max))
				{
					per=tax.get(r);
				}
			}
			taxa=per*tsal/100;
			String data=sal+","+gender+","+taxa;
			context.write(new Text("->"), new Text(data));
		}
		else
		{
			String data=sal+","+gender+","+taxa;
			context.write(new Text("->"), new Text(data));
		}
		}
	}
