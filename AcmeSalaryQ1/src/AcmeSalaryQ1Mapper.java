import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeSalaryQ1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Q1
		/* Steps
		 * Get single row
		 * Split row into array by comma
		 * Get index of contract type
		 * Use contract type as key
		 * associate 1 as the value
		 */
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
				return;
			else {
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int contract_index = 4;
				String contract = line_arr[contract_index];
				
				context.write(new Text(contract), new IntWritable(1));
			}
		} catch (Exception e) {
		    e.printStackTrace();
		}
	}
}
