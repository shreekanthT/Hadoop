package Demo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask2 extends Mapper<LongWritable,Text,Text,DoubleWritable>
{
private Map<String, Integer> orphan = new HashMap<String, Integer>();
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException 
	{
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
		for (Path SinglePath : files) {
			//if (SinglePath.getName().equals("/user/cloudera/orphan_table/part-m-00000")) 
			//{
				BufferedReader reader = new BufferedReader(new FileReader(SinglePath.toString()));
			String line="";
			while((line=reader.readLine())!=null)
			{
				String data[]=line.split(",");
				String cat=data[0];
				int per=Integer.parseInt(data[1]);
				orphan.put(cat,per);
			//}
			}
			reader.close();
		}
		if (orphan.isEmpty()) 
		{
			throw new IOException("Unable To Load Data from Orphan table .");
		}
	}

	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException 
			{
		String[] tData=value.toString().split(",");
		//int ey=context.getConfiguration().getInt("cheackyear",0);
		String parents=tData[6];
		double per=0.0;
		if(parents.contains("Both"))
		{
		}
		else
		{
			if(parents.contains("Not"))
			{
				per=(double)orphan.get("C3");
				context.write(new Text("Total Scholarship Amount : "), new DoubleWritable(per));
			}
			else if(parents.contains("Mother"))
			{
				per=(double)orphan.get("C2");
				context.write(new Text("Total Scholarship Amount : "), new DoubleWritable(per));
			}
			else
			{
				per=(double)orphan.get("C1");
				context.write(new Text("Total Scholarship Amount : "), new DoubleWritable(per));
			}
		}
			
		}
		}
