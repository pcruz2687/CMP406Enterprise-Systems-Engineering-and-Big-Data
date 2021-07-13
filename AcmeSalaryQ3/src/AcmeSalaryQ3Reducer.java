import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeSalaryQ3Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		DecimalFormat df2 = new DecimalFormat("#.##");
		double total_salary = 0;
		int service_type_count = 0;
		double service_average_salary = 0;
		String service_average_salary_2f = "";
		
		// Add all salaries in each service type
		// Count salaries in the service type
		for(IntWritable value : values) {
			total_salary += value.get();
			service_type_count++;
		}
		
		// Calculate the average salary in each service type up to 2 decimal places
		service_average_salary = total_salary / service_type_count;
		service_average_salary_2f = df2.format(service_average_salary);
		
		context.write(new Text(key + ","), new DoubleWritable(Double.parseDouble(service_average_salary_2f)));
	}
}
