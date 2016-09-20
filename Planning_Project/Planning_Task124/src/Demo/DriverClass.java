package Demo;
import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DriverClass 
{
public static void main(String []arg) throws IOException, InterruptedException, ClassNotFoundException
{
	Configuration con=new Configuration();
	FileSystem hdfs = FileSystem.get(con);
	Job job=new Job(con,"Census demo");//throws IOException
	job.setJarByClass(DriverClass.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(ReducerClass.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	int eage,ey;
	Scanner sc=new Scanner(System.in);
	System.out.print("Planning Menu\n==========================\n1.Voter(s) count in x year(s)\n2.Senior Citizen(s) count in x year(s)\n3.Citizens and immigrants count for employed lot\nEnter your choice : ");
	int ch=sc.nextInt();
	
	switch(ch)
	{
	case 1:
	{
	System.out.print("Enter the eligible age for voting : ");
	eage=sc.nextInt();
	System.out.print("Enter the years for projection check : ");
	ey=sc.nextInt();
	con.setInt("eligibleyear",eage);
	con.setInt("cheackyear",ey);
	job.setMapperClass(MapperTask1.class);
	break;
	}
	case 2:
	{
	System.out.print("Enter the eligible age for senior citizen category : ");
	eage=sc.nextInt();
	System.out.print("Enter the years for projection check : ");
	ey=sc.nextInt();
	con.setInt("eligibleyear",eage);
	con.setInt("cheackyear",ey);
	job.setMapperClass(MapperTask2.class);
	break;
	}
	case 3:
	{
	job.setMapperClass(MapperTask3.class);
	break;
	}
	}
	
	FileInputFormat.addInputPath(job,new Path(arg[0]));
	Path newFolderPath = new Path(arg[1]);
	try
	{
	FileInputFormat.addInputPath(job,new Path(arg[0]));
	if (hdfs.exists(newFolderPath)) 
	{
		hdfs.delete(newFolderPath, true); 
	}
	}
	catch(Exception ex)
	{
		Logger ob=Logger.getLogger("errorlog");
		ob.log(Level.SEVERE,"Error found for : {0}",ex.getMessage());
	}
	FileOutputFormat.setOutputPath(job,newFolderPath);
	if(job.waitForCompletion(true))
	{
		hdfs.copyToLocalFile(new Path(arg[1]),new Path("/home/cloudera/"));
	}
}
}
