package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VolumeMapper extends Mapper<LongWritable, Text,
Text, LongWritable> {
	
	

	
    @Override
    protected void map(LongWritable key, Text value,
            Context context)
    throws
    IOException, InterruptedException {
        String document = value.toString();
        
        long volume = 0;
        String date = null;

        	
        	Pattern volumePattern = Pattern.compile("<E6>(\\d*)</E6>");
        	Matcher volumeMatcher=volumePattern.matcher(document);
        	// 1 group - Year
        	// 2 group - Month
        	// 3 group - Day
        	// 4 group - Hour
        	Pattern detalizationPattern = Pattern.compile("<E41>([^-]*)-([^-]*)-([^-]*)\\s([^:]*).*</E41>");
        	Matcher detalizationMatcher=detalizationPattern.matcher(document);
        	
        	if (volumeMatcher.find() && detalizationMatcher.find()){
        		
        		volume=Long.parseLong(volumeMatcher.group(1));
        		date=detalizationMatcher.group(1)+detalizationMatcher.group(2)+detalizationMatcher.group(3)+detalizationMatcher.group(4);
        		
        	}else {
        		return;
        	}
        	
        	context.write(new Text(date), new LongWritable(volume));
            
        }
    }



