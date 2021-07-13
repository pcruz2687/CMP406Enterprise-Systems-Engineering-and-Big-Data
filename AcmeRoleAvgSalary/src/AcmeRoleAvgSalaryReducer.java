import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeRoleAvgSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		DecimalFormat df2 = new DecimalFormat("#.##");
		double total_salary = 0;
		int role_salary_counter = 0;
		
		// Add all salaries per role and contract type
		// Count the salaries for the role
		for(IntWritable value : values) {
			total_salary += value.get();
			role_salary_counter++;
		}
		
		// Get the average salary for the role up to 2 decimal places
		double role_average_salary = total_salary / role_salary_counter;
		String role_average_salary_2f = df2.format(role_average_salary);
		
		context.write(new Text(key + ","), new DoubleWritable(Double.parseDouble(role_average_salary_2f)));
	}
}
