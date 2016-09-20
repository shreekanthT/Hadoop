package Demo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text,Text,Text,IntWritable>
{

public void reduce(Text inkey,Iterable<Text> invals,Context context) throws InterruptedException, IOException
{
	int count=0;
	for(@SuppressWarnings("unused") Text a:invals)
	{
		count++;
	}
	context.write(new Text(inkey),new IntWritable(count));
}
}
