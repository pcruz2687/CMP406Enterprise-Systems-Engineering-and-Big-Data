import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeOverpaidStaffReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		// Count the overpaid staff
		for(IntWritable value : values) {
			count += value.get();
		}
		
		context.write(new Text(key + ","), new IntWritable(count));
	}
}
