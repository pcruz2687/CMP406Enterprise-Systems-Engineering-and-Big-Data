import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AcmeOverpaidStaffMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public static String[][] avg_salary_arr;
	
	// Pre-process the average salary data
	public void setup(Context context) throws IOException {
		// Get the role average salary from the AcmeRoleAvgSalary MapReduce code
		Path path = new Path("/user/cloudera/role-contract-avg-salary/part-r-00000");
		FileSystem filesystem = FileSystem.get(new Configuration());
		BufferedReader buffer_reader_1 = new BufferedReader(new InputStreamReader(filesystem.open(path)));
		BufferedReader buffer_reader_2 = new BufferedReader(new InputStreamReader(filesystem.open(path)));
		
		int rows = 0;
		int columns = 2;
		int current_row = 0;
		
		// Get array rows size by counting the records in the part-r-00000 file
		String line_1;
		line_1 = buffer_reader_1.readLine();
		while (line_1 != null){
			rows++;
			line_1 = buffer_reader_1.readLine();
		}
		buffer_reader_1.close();
		
		// Create the 2D array using the rows and columns variables
		avg_salary_arr = new String [rows][columns];
		
		// Add each record inside the part-r-00000 file into the 2D array
		String line_2;
		line_2 = buffer_reader_2.readLine();
	    while (line_2 != null){
			String[] line_arr = line_2.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			line_2 = buffer_reader_2.readLine();
			avg_salary_arr[current_row][0] = line_arr[0] + "," + line_arr[1];
			avg_salary_arr[current_row][1] = line_arr[2];
			current_row++;
	    }
	    buffer_reader_2.close();
	}	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore the header
				return;
			else {
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int role_index = 2;
				int contract_index = 4;
				int salary_index = 5;
				String role_and_contract = line_arr[role_index] + ", " + line_arr[contract_index];
				int salary = Integer.parseInt(line_arr[salary_index]);
				
				// Get the index of the role and contract
				int role_avg_salary_index = binary_search(avg_salary_arr, role_and_contract);
				String role_str_comparison = avg_salary_arr[role_avg_salary_index][0];
				double role_avg_salary = Double.parseDouble(avg_salary_arr[role_avg_salary_index][1]);
				
				// Compare the current role and contract with the index returned by the binary search
				// Employee is overpaid if salary is over 25% the average salary
				if(role_and_contract.compareTo(role_str_comparison) == 0) {
					if(salary > (role_avg_salary * 1.25)) {
						context.write(new Text(line), new IntWritable(1));
					}
				}
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	// Binary Search to find the index of the given concatenation of role and contract type 
	public static int binary_search(String[][] avg_salary_arr, String role_and_contract) {
		int low = 0;
		int high = avg_salary_arr.length - 1;
		
		while(low <= high){
			int mid = (int) Math.floor((low + high) / 2);
			int guess = role_and_contract.compareTo(avg_salary_arr[mid][0]);
			if(guess == 0){
				return mid;
			} else if (guess > 0) {
				low = mid + 1;
			} else {
				high = mid - 1;
			}
		}
		return 1;
	}
}
