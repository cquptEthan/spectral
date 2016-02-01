package edu.cqupt.spectral.lanczos;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeReducer;
import edu.cqupt.spectral.diagonalize.DiagonazlizeMapper;
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
 * Date: 1/22/16
 * Time: 9:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class LanczosJob {
    public static void runJob(Configuration conf)
            throws IOException, ClassNotFoundException, InterruptedException {
        String sourceTable = Tools.AFFINITY_TABLE_NAME;
        String targetTable = Tools.DIAGONALIZE_TABLE_NAME;
        // set up all the job tasks
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "DiagonalizeJob");
        Scan scan = new Scan();
//        FileInputFormat.addInputPath(job, affInput);
//        FileOutputFormat.setOutputPath(job, diagOutput);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                DiagonazlizeMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                DoubleWritable.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                DiagonalizeReducer.class,    // reducer class
                job);
        job.setJarByClass(LanczosJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
    }
}
