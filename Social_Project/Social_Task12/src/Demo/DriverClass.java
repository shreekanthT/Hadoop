package Demo;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

	int eage,ey;
	Scanner sc=new Scanner(System.in);
	System.out.print("Social Menu\n==========================\n1.Total amount dispensed on pension in x year(s)\n2.Total amount dispensed on scholarship in current year\nEnter your choice : ");
	int ch=sc.nextInt();
	
	switch(ch)
	{
	case 1:
	{
	System.out.print("Enter the years for projection check : ");
	ey=sc.nextInt();
	System.out.print("Enter the min age for senior citizen category : ");
	eage=sc.nextInt();
	con.setInt("cheackyear",ey);
	con.setInt("age",eage);
	Job job=new Job(con,"Census demo");//throws IOException
	job.setJarByClass(DriverClass.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(ReducerClass.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	try 
	{
		DistributedCache.addCacheFile(new URI("/user/cloudera/pension_table/part-m-00000"),job.getConfiguration());
	} 
	catch (URISyntaxException e) 
	{
		e.printStackTrace();
	}
	job.setMapperClass(MapperTask1.class);
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
	break;
	}
	
	case 2:
	{
//	System.out.print("Enter the years for projection check : ");
//	ey=sc.nextInt();
//	con.setInt("cheackyear",ey);
		Job job=new Job(con,"Census demo");//throws IOException
		job.setJarByClass(DriverClass.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
	try 
	{
		DistributedCache.addCacheFile(new URI("/user/cloudera/orphan_table/part-m-00000"),job.getConfiguration());
	} 
	catch (URISyntaxException e) 
	{
		e.printStackTrace();
	}
	job.setMapperClass(MapperTask2.class);
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
	break;
	}
	
	default:
	{
	System.exit(0);
	}
	}
}
}
