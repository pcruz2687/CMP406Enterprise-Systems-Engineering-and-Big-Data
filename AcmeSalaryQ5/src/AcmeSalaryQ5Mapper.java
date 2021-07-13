import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeSalaryQ5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Q5
		/* Steps
		 * Get single row
		 * Split row into array by comma
		 * Get index of salary
		 * Use generic string as key
		 * Use salary as value
		 */
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
				return;
			else {
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String generic_key = "Staff";
				int salary_index = 5;
				int salary = Integer.parseInt(line_arr[salary_index]);
				
				context.write(new Text(generic_key), new IntWritable(salary));
			}
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}
