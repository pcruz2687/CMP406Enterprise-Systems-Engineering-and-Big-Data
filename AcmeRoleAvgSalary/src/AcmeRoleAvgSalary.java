import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AcmeRoleAvgSalary {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage: AcmeRoleAvgSalary <input path> <output path>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "acmesalary");
		job.setJarByClass(AcmeRoleAvgSalary.class);
		job.setJobName("Acme Role Average Salary");
		
		// Set the input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Specify the appropriate class for the Mapper and Reducer
		job.setMapperClass(AcmeRoleAvgSalaryMapper.class);
		job.setReducerClass(AcmeRoleAvgSalaryReducer.class);
		
		// Set the data type of the output key value pairs
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
