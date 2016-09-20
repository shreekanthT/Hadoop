package Demo;
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask2 extends Mapper<LongWritable,Text,Text,Text>
{
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//1||fail
		String[] tData=value.toString().split(",");
		int tage=Integer.parseInt(tData[0]);
		String gender=tData[3];
		int ea,ey;
		ea=context.getConfiguration().getInt("eligibleyear",0);
		ey=context.getConfiguration().getInt("cheackyear",0);
		tage=tage+ey;
		if(tage>=ea)
		{
			if(gender.equals(" Female"))
			{
		context.write(new Text("Female voter(s) after "+ey+" year(s) : "), new Text("Passed"));
			}
			else
			{
		context.write(new Text("Male voter(s) after "+ey+" year(s) : "), new Text("Passed"));
			}
		}
	}
}
