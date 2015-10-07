package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MediaDurationMapper extends Mapper<LongWritable, Text,
Text, LongWritable> {
	

	
    @Override
    protected void map(LongWritable key, Text value,
            Context context)
    throws
    IOException, InterruptedException {
    	
    	String document = value.toString();
    	
    	 long md = 0;
         String date = null;
         String minutes = null;
         String minute = null;

         	
         	Pattern mdPattern = Pattern.compile("<E177>(\\d*)</E177>");
         	Matcher mdMatcher=mdPattern.matcher(document);
         	// 1 group - Year
         	// 2 group - Month
         	// 3 group - Day
         	// 4 group - Hour
         	Pattern detalizationPattern = Pattern.compile("<E41>([^-]*)-([^-]*)-([^-]*)\\s([^:]*):(\\d{2}).*</E41>");
         	Matcher detalizationMatcher=detalizationPattern.matcher(document);
         	
         	if (mdMatcher.find() && detalizationMatcher.find()){
         		
         		md=Long.parseLong(mdMatcher.group(1));
         		minutes=detalizationMatcher.group(5);
         		minute = minutes.matches("\\d{1}[0-4]") ? "0" : "5";
         		date=detalizationMatcher.group(1)+detalizationMatcher.group(2)+detalizationMatcher.group(3)+detalizationMatcher.group(4)+
         				minutes.substring(0,1)+minute;
         		context.write(new Text(date+":"+md), new LongWritable(1));
         		
         	}else {
         		return;
         	}
        	
        	
            
        }
    }



