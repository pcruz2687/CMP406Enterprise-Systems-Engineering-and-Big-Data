import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeSalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
//		try {
//			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
//				return;
//			else {
//				String line = value.toString();
//				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//				int contract_index = 4;
//				String contract = line_arr[contract_index];
//				
//				context.write(new Text(contract), new IntWritable(1));
//				}
//		} catch (Exception e) {
//		    e.printStackTrace();
//		}
		
		// Q2
//		/* Steps
//		 * Get single row
//		 * Split row into array by comma
//		 * Get index of service type
//		 * Use service type as key
//		 * associate 1 as the value
//		 */
//		try {
//			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
//				return;
//			else {
//				String line = value.toString();
//				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//				int service_type_index = 3;
//				String service_type = line_arr[service_type_index];
//				
//				context.write(new Text(service_type), new IntWritable(1));
//			}
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
		
		//Q3
//		/* Steps
//		 * Get single row
//		 * Split row into array by comma
//		 * Get index of service type
//		 * Get index of salary
//		 * Use service type as key
//		 * Use salary as value
//		 */
//		try {
//			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
//				return;
//			else {
//				String line = value.toString();
//				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//				int service_type_index = 3;
//				int salary_index = 5;
//				String service_type = line_arr[service_type_index];
//				int salary = Integer.parseInt(line_arr[salary_index]);
//				
//				context.write(new Text(service_type), new IntWritable(salary));
//			}
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

		// Q4
//		/* Steps
//		 * Get single row
//		 * Split row into array by comma
//		 * Get index of job role
//		 * Use job role as key
//		 * associate 1 as the value
//		 */

//		try {
//			if (key.get() == 0 && value.toString().contains("header") // Ignore header
//				return;
//			else {
//				String line = value.toString();
//				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//				int role_index = 2;
//				String role = line_arr[role_index];
//				
//				context.write(new Text(role), new IntWritable(1));
//			}
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
		
		//Q5
		/* Steps
		 * Get single row
		 * Split row into array by comma
		 * Get index of salary
		 * Use generic string as key
		 * Use salary as value
		 */
		
//		try {
//			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
//				return;
//			else {
//				String line = value.toString();
//				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//				String generic_key = "Staff";
//				int salary_index = 5;
//				int salary = Integer.parseInt(line_arr[salary_index]);
//				
//				context.write(new Text(generic_key), new IntWritable(salary));
//			}
//	    } catch (Exception e) {
//	        e.printStackTrace();
//	    }
	}
}
