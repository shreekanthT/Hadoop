package Demo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text,DoubleWritable,Text,Text>
{

public void reduce(Text inkey,Iterable<DoubleWritable> invals,Context context) throws InterruptedException, IOException
{
	double sum=0.0;
	for(DoubleWritable a:invals)
	{
		double temp=a.get();
		sum+=temp;
	}
	Double d=new Double(sum);
	String data=d.toString();
	context.write(new Text(inkey),new Text(data));
}
}
