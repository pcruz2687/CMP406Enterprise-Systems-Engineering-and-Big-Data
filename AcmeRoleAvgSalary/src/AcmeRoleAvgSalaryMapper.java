import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeRoleAvgSalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore the header
				return;
			else {
				// Split the record by the comma delimiter but not if it's within double quotes
				// then store it in an array to access the required columns via index number
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int role_index = 2;
				int contract_index = 4;
				int salary_index = 5;
				String role = line_arr[role_index];
				String contract = line_arr[contract_index];
				int salary = Integer.parseInt(line_arr[salary_index]);
				
				context.write(new Text(role + ", " + contract), new IntWritable(salary));
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}


