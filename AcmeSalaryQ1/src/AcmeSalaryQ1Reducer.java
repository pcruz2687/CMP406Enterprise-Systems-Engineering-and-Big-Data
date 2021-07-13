import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeSalaryQ1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// Add up the values to count
		// the number of staff working in full and part-time contract
		int count = 0;
		for(IntWritable value : values) {
			count += value.get();
		}
		context.write(new Text(key + ","), new IntWritable(count));	
	}
}
