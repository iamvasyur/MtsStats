package iam.hadooper.com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class YvXmlInputFormat extends TextInputFormat {
	private FSDataInputStream fsin;
	
	public final String START_TAG_KEY = "xmlinput.start";
    public final String END_TAG_KEY = "xmlinput.end";
	
	public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new YvXmlRecordReader();
    }
	
	public class YvXmlRecordReader extends RecordReader<LongWritable, Text> {
		
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		
		private DataOutputBuffer buffer = new DataOutputBuffer();
		
		private long start;
        private long end;
        
        private byte[] startTag;
        private byte[] endTag;

		@Override
		public void close() throws IOException {
			fsin.close();
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			
			Configuration conf = arg1.getConfiguration();
			
			FileSplit inputFileSplit = (FileSplit)arg0;
			start=inputFileSplit.getStart();
			end=start+inputFileSplit.getLength();
			
			startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
            endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
			
			FileSystem fs = inputFileSplit.getPath().getFileSystem(conf);
			
			fsin = fs.open(inputFileSplit.getPath());
            fsin.seek(start);	
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
	           if (fsin.getPos() < end) {
	                if (readUntilMatch(startTag, false)) {
	                    try {
	                        buffer.write(startTag);
	                        if (readUntilMatch(endTag, true)) {
	                            key.set(fsin.getPos());
	                            value.set(buffer.getData(), 0,
	                                    buffer.getLength());
	                            return true;
	                        }
	                    } finally {
	                        buffer.reset();
	                    }
	                }
	            }
	            return false;
		}
		private boolean readUntilMatch(byte[] match, boolean withinBlock)
		        throws IOException {
		            int i = 0;
		            while (true) {
		                int b = fsin.read();
		                // end of file:
		                    if (b == -1)
		                        return false;
		                // save to buffer:
		                    if (withinBlock)
		                        buffer.write(b);
		                // check if we're matching:
		                    if (b == match[i]) {
		                        i++;
		                        if (i >= match.length)
		                            return true;
		                    } else
		                        i = 0;
		                    // see if we've passed the stop point:
		                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
		                        return false;
		            }
		        }
		
	}

}
