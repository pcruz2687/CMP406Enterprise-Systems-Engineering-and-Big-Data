import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AcmeSalaryQ2 {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage: AcmeSalaryQ2 <input path> <output path>");
			System.exit(0);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "acmesalary");
		job.setJarByClass(AcmeSalaryQ2.class);
		job.setJobName("Acme Salary Q2");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AcmeSalaryQ2Mapper.class);
		job.setReducerClass(AcmeSalaryQ2Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
