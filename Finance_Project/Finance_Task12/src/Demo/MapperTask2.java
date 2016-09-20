package Demo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask2 extends Mapper<LongWritable,Text,Text,Text>
{
	private Map<Integer, String> map = new HashMap<Integer, String>();
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
	for (Path SinglePath : files) {
		//if (SinglePath.getName().equals("/user/cloudera/agegroup.dat")) 
		//{
			BufferedReader reader = new BufferedReader(new FileReader(SinglePath.toString()));
		String line="";
		while((line=reader.readLine())!=null)
		{
			String data[]=line.split("\\t");
			int age=Integer.parseInt(data[0].trim());
			String cat=data[1].trim();
			map.put(age,cat);
		}
		reader.close();
		//}
	}
	if (map.isEmpty()) 
	{
		throw new IOException("Unable To Load Agegroup Data .");
	}
	}
	
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException 
			{
		String[] tData=value.toString().split(",");
		String gender=tData[3].trim();
		int age=Integer.parseInt(tData[0]);
		double tsal=Double.parseDouble(tData[5])*12;
		String tcat=map.get(age);
		
		int sal=(int)tsal;
			String data=sal+","+gender+","+tcat;
			context.write(new Text("->"), new Text(data));
		}

}
