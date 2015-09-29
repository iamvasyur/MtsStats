package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VolumeMapper extends Mapper<LongWritable, Text,
Text, LongWritable> {
	
	public Log LOG = LogFactory.getLog(VolumeMapper.class);

	
    @Override
    protected void map(LongWritable key, Text value,
            Context context)
    throws
    IOException, InterruptedException {
        String document = value.toString();
        
        long volume = 0;
        String date = null;
        String minutes = null;
        String minute = null;

        	
        	Pattern volumePattern = Pattern.compile("<E6>(\\d*)</E6>");
        	Matcher volumeMatcher=volumePattern.matcher(document);
        	// 1 group - Year
        	// 2 group - Month
        	// 3 group - Day
        	// 4 group - Hour
        	Pattern detalizationPattern = Pattern.compile("<E41>([^-]*)-([^-]*)-([^-]*)\\s([^:]*):(\\d{2}).*</E41>");
        	Matcher detalizationMatcher=detalizationPattern.matcher(document);
        	
        	if (volumeMatcher.find() && detalizationMatcher.find()){
        		
        		volume=Long.parseLong(volumeMatcher.group(1));
        		minutes=detalizationMatcher.group(5);
        		minute = minutes.matches("\\d{1}[0-4]") ? "0" : "5";
        		date=detalizationMatcher.group(1)+detalizationMatcher.group(2)+detalizationMatcher.group(3)+detalizationMatcher.group(4)+
        				minutes.substring(0,1)+minute;
        		context.write(new Text(date), new LongWritable(volume));
        		
        	}else {
        		LOG.info("====   "+document);
        		return;
        	}
        	
        	
            
        }
    }



