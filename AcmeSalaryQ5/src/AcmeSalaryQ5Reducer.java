import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class AcmeSalaryQ5Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		double total_salary = 0;
		int staff_count = 0;
		int below_average_salary_count = 0;
		List<IntWritable> cache = new ArrayList<IntWritable>();
		
		// Copy the values of the list into another list called "cache"
		// because a reducer can only iterate on the list once
		for(IntWritable value : values) {
			total_salary += value.get();
			staff_count++;
			cache.add(new IntWritable(value.get()));
		}
		// Calculate the average salary of the university as a whole
		int average_salary = (int) (total_salary / staff_count);
		
		// Use the cache list to check if the salary is below
		// the university average salary
		for(IntWritable value : cache) {
			if(value.get() < average_salary) {
				below_average_salary_count++;
			}
		}
		context.write(new Text("Staff Below Average Salary"), new IntWritable(below_average_salary_count));	
	}
}
