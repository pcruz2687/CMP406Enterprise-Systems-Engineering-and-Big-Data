import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeSalaryQ3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/* Steps
		 * Get single row
		 * Split row into array by comma
		 * Get index of service type
		 * Get index of salary
		 * Use service type as key
		 * Use salary as value
		 */
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
				return;
			else {
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int service_type_index = 3;
				int salary_index = 5;
				String service_type = line_arr[service_type_index];
				int salary = Integer.parseInt(line_arr[salary_index]);
				
				context.write(new Text(service_type), new IntWritable(salary));
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
