package edu.cqupt.spectral.laplacian;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeReducer;
import edu.cqupt.spectral.diagonalize.DiagonazlizeMapper;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/20/16
 * Time: 6:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class LaplacianJob {
    public static void runJob(Path affInput)
            throws IOException, ClassNotFoundException, InterruptedException {
        String sourceTable = Tools.AFFINITY_TABLE_NAME;
        String targetTable = Tools.LAPLACIAN_TABLE_NAME;
        // set up all the job tasks
        Configuration conf = new Configuration();
        Path diagOutput = new Path(affInput.getParent(), "diagonal");
        HadoopUtil.delete(conf, diagOutput);
        Job job = new Job(conf, "LaplacianJob");
        Scan scan = new Scan();
        FileInputFormat.addInputPath(job, affInput);
        FileOutputFormat.setOutputPath(job, diagOutput);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                LaplacianMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                IntDoublePairWritable.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                LaplacianReducer.class,    // reducer class
                job);
        job.setJarByClass(LaplacianJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
    }
}
