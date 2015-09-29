package iam.hadooper.com;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Starter {
	
	public static Log LOG = LogFactory.getLog(Starter.class);
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		try {
			conf.set("xmlinput.start", "<TE");
		    conf.set("xmlinput.end", "</TE");
		} catch (Exception exec){
			LOG.info("Usage: MtsStat.jar [inputFile in HDFS] [outputFile to HDFS] [KPI] [Detalization];");
			LOG.info("KPI = [Delta, Volume]");
			LOG.info("KPI = [Hour, Day]");
			return;
		}
	    
	    Job job = Job.getInstance(conf);
		job.setJarByClass(Starter.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapperClass(VolumeMapper.class);
	    job.setReducerClass(VolumeReducer.class);
	    
	    

	    job.setInputFormatClass(YvXmlInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);	
	}
	
}
