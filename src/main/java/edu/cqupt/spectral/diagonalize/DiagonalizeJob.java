package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/18/16
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class DiagonalizeJob {
    public static Vector runJob(Path affInput, int dimensions)
            throws IOException, ClassNotFoundException, InterruptedException {

        // set up all the job tasks
        Configuration conf = new Configuration();
        Path diagOutput = new Path(affInput.getParent(), "diagonal");
        HadoopUtil.delete(conf, diagOutput);
        conf.setInt("edu.cqupt.spectral.dimensions", dimensions);
        Job job = new Job(conf, "DiagonalizeJob");

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntDoublePairWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(DiagonazlizeMapper.class);
        job.setReducerClass(DiagonalizeReducer.class);

        FileInputFormat.addInputPath(job, affInput);
        FileOutputFormat.setOutputPath(job, diagOutput);

        job.setJarByClass(DiagonalizeJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
        return Tools.load(conf, new Path(diagOutput, "part-r-00000"));
    }
}
