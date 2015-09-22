package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeltaMapper extends Mapper<LongWritable, Text,
IntWritable, IntWritable> {

	
    @Override
    protected void map(LongWritable key, Text value,
            Context context)
    throws
    IOException, InterruptedException {
    	int delta =0;
        String document = value.toString();
        Pattern pacedDurationPattern=Pattern.compile("<E178>(\\d*)</E178>");
        Matcher pacedDurationMatcher=pacedDurationPattern.matcher(document);
        
        Pattern durationPattern=Pattern.compile("<E177>(\\d*)</E177>");
        Matcher durationMatcher=durationPattern.matcher(document);
        
        if  ( pacedDurationMatcher.find() && durationMatcher.find()){
        	delta = Integer.parseInt(durationMatcher.group(1))-Integer.parseInt(pacedDurationMatcher.group(1));
        	
        }else {
        	delta=-111;
        }
        
        
        context.write(new IntWritable(delta), new IntWritable(1));
        }
    }


