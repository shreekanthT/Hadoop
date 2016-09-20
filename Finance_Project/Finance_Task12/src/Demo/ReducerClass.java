package Demo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text,Text,Text,Text>
{

public void reduce(Text inkey,Iterable<Text> invals,Context context) throws InterruptedException, IOException
{
	double ti=0,tt=0,tm=0,tf=0;
	int c=0,cm=0,cf=0;
	
	for(Text a:invals)
	{
		String data=a.toString();
		String tData[]=data.split(",");
		double income=Double.parseDouble(tData[0]);
		ti+=income;
		c++;
		String gender=tData[1];
		double tax=Double.parseDouble(tData[2].trim());
		if(tax>0.0)
		{
		if(gender.contains("Male"))
		{
			cm++;
			tm+=tax;
		}
		else
		{
			cf++;
			tf+=tax;
		}
		tt+=tax;
		}
	}
	String data="Total income : "+ti+"\n\tTotal population : "+c+"\n\tTotal tax payer(s) : "+(cm+cf)+"\n\tMale tax payer(s) : "+cm+"\n\tFemale tax payer(s) : "+cf+"\n\tTotal tax collected : "+tt+"\n\tTotal tax collected from male(s) : "+tm+"\n\tTotal tax collected from female(s) : "+tf;
	context.write(new Text(""),new Text(data));
}
}
