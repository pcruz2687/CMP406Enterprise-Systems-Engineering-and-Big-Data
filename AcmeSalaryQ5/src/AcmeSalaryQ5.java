import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AcmeSalaryQ5 {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage: AcmeSalaryQ5 <input path> <output path>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "acmesalary");
		job.setJarByClass(AcmeSalaryQ5.class);
		job.setJobName("Acme Salary Q5");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AcmeSalaryQ5Mapper.class);
		job.setReducerClass(AcmeSalaryQ5Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
