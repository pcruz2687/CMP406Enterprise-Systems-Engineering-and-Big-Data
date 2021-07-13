import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// Make sure output is IntWritable
		// Question 1, 2, 4
//		int count = 0;
//		for(IntWritable value : values) {
//			count += value.get();
//		}
//		context.write(new Text(key + ","), new IntWritable(count));	
		
		// Make sure output is DoubleWritable
		// Question 3
		DecimalFormat df2 = new DecimalFormat("#.##");
		double total_salary = 0;
		int service_type_count = 0;
		double role_average_salary = 0;
		String role_average_salary_2f = "";
		
		// Add all salaries in each service type
		// Count salaries in the service type
		for(IntWritable value : values) {
			total_salary += value.get();
			service_type_count++;
		}
		
		// Calculate the average salary in each service type up to 2 decimal places
		role_average_salary = total_salary / service_type_count;
		role_average_salary_2f = df2.format(role_average_salary);
		
		context.write(new Text(key + ","), new DoubleWritable(Double.parseDouble(role_average_salary_2f)));
		
//		// Question 5
//		double total_salary = 0;
//		int staff_count = 0;
//		int below_average_salary_count = 0;
//		List<IntWritable> cache = new ArrayList<IntWritable>();
//		
//		for(IntWritable value : values) {
//			total_salary += value.get();
//			staff_count++;
//			cache.add(new IntWritable(value.get()));
//		}
//		int average_salary = (int) (total_salary / staff_count);
//		
//		for(IntWritable value : cache) {
//			if(value.get() < average_salary) {
//				below_average_salary_count++;
//			}
//		}
//		context.write(new Text("Staff Below Average Salary"), new IntWritable(below_average_salary_count));	
	}
}
