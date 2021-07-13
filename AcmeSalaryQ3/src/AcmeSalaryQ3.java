import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AcmeSalaryQ3 {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage: AcmeSalaryQ3 <input path> <output path>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "acmesalary");
		job.setJarByClass(AcmeSalaryQ3.class);
		job.setJobName("Acme Salary Q3");

		// Set the input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Specify the appropriate class for the Mapper and Reducer
		job.setMapperClass(AcmeSalaryQ3Mapper.class);
		job.setReducerClass(AcmeSalaryQ3Reducer.class);

		// Set the data type of the output key value pairs
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
