package Demo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask1 extends Mapper<LongWritable,Text,Text,DoubleWritable>
{
	private Map<RangeHandler, Integer> pension = new HashMap<RangeHandler, Integer>();
	
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
				pension.put(ob,per);
			//}
			}
			reader.close();
		}
		if (pension.isEmpty()) 
		{
			throw new IOException("Unable To Load Data from Pension table .");
		}
	}

	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException 
			{
		String[] tData=value.toString().split(",");
		int tage=Integer.parseInt(tData[0]);
		int ey,eage;
		ey=context.getConfiguration().getInt("cheackyear",0);
		eage=context.getConfiguration().getInt("age",0);
		tage=tage+ey;
		double tsal=Double.parseDouble(tData[5]);
		int sal=(int)tsal;
		double p=0.0;
		int per=0;
		Set<RangeHandler> keyP=pension.keySet();
		if(tage>=eage)
		{
			for(RangeHandler r:keyP)
			{
				int min=r.getMin();
				int max=r.getMax();
				if((sal>min) && (sal<max))
				{
					per=pension.get(r);
				}
			}
			p=per*tsal/100;
			String data="Total Pension Amount after "+ey+" year(s) : ";
			context.write(new Text(data), new DoubleWritable(p));
		}
		}
	}
