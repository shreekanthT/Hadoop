package Demo;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask3 extends Mapper<LongWritable,Text,Text,Text>
{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//1||fail
		String[] tData=value.toString().split(",");
		String status=tData[8];
		int weeks=Integer.parseInt(tData[9]);
		if(weeks!=0)
		{
		if(status.contains(" Foreign"))
		{
		context.write(new Text("Immigrants :"), new Text("Passed"));
		}
		else
		{
			context.write(new Text("Citizens :"), new Text("Passed"));	
		}
		}
		else
		{
			
		}
	}
}
