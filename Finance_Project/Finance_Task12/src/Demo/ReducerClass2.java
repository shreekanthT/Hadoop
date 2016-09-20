package Demo;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass2 extends Reducer<Text,Text,Text,Text>
{

public void reduce(Text inkey,Iterable<Text> invals,Context context) throws InterruptedException, IOException
{
	double t=0.0,tf=0.0,tm=0.0,ti=0.0,tt=0.0,ta=0.0,tma=0.0,ts=0.0,te=0.0;
	int c=0,cm=0,cf=0,ci=0,ct=0,ca=0,cma=0,cs=0,ce=0;
	double pci=0.0,pcii=0.0,pcit=0.0,pcia=0.0,pcima=0.0,pcis=0.0,pcie=0.0,pcim=0.0,pcif=0.0;
	for(Text a:invals)
	{
		String data=a.toString();
		String tData[]=data.split(",");
		double sal=Double.parseDouble(tData[0]);
		String gender=tData[1].trim();
		String cat=tData[2];
		t+=sal;
		c++;
		
		if(gender.equals("Male"))
		{
			cm++;
			tm+=sal;
		}
		else
		{
			cf++;
			tf+=sal;
		}
		
		if(cat.contains("infants"))
		{
			ci++;
			ti+=sal;
		}
		else if(cat.contains("Teenager"))
		{
			ct++;
			tt+=sal;
		}
		else if(cat.contains("adult"))
		{
			ca++;
			ta+=sal;
		}
		else if(cat.contains("middle"))
		{
			cma++;
			tma+=sal;
		}
		else if(cat.contains("senior"))
		{
			cs++;
			ts+=sal;
		}
		else
		{
			ce++;
			te+=sal;
		}
	}
	pci=t/c;
	pcim=tm/cm;
	pcif=tf/cf;
	pcii=ti/ci;
	pcit=tt/ct;
	pcia=ta/ca;
	pcima=tma/cma;
	pcis=ts/cs;
	pcie=te/ce;
	
	String data="\nCategory : Consolidated\nPer Capita Income : "+pci+"\nCount : "+c+"\n==========\nCategory : Gender(Male)\nPer Capita Income : "
	+pcim+"\nCount : "+cm+"\n==========\nCategory : Gender(Female)\nPer Capita Income : "+pcif+"\nCount : "+cf+
	"\n==========\nCategory : AgeGroup(Infant)\nPer Capita Income : "+pcii+"\nCount : "+ci+
	"\n==========\nCategory : AgeGroup(Teenager)\nPer Capita Income : "+pcit+"\nCount : "+ct+
	"\n==========\nCategory : AgeGroup(Adult)\nPer Capita Income : "+pcia+"\nCount : "+ca+
	"\n==========\nCategory : AgeGroup(Middle-Aged)\nPer Capita Income : "+pcima+"\nCount : "+cma+
	"\n==========\nCategory : AgeGroup(Senior Citizen)\nPer Capita Income : "+pcis+"\nCount : "+cs+
	"\n==========\nCategory : AgeGroup(Elderly)\nPer Capita Income : "+pcie+"\nCount : "+ce;
	context.write(new Text(""),new Text(data));
}
}
