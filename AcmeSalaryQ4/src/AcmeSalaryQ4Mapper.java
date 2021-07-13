import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class AcmeSalaryQ4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//		/* Steps
//		 * Get single row
//		 * Split row into array by comma
//		 * Get index of job role
//		 * Use job role as key
//		 * associate 1 as the value
//		 */
		try {
			if (key.get() == 0 && value.toString().contains("header")) // Ignore header
				return;
			else {
				String line = value.toString();
				String[] line_arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int role_index = 2;
				String role = line_arr[role_index];
				
				context.write(new Text(role), new IntWritable(1));
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
