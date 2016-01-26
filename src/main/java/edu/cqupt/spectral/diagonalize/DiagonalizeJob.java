package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
    public static void runJob(Path affInput)
            throws IOException, ClassNotFoundException, InterruptedException {
        String sourceTable = Tools.AFFINITY_TABLE_NAME;
        String targetTable = Tools.DIAGONALIZE_TABLE_NAME;
        // set up all the job tasks
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Path diagOutput = new Path(affInput.getParent(), "diagonal");
        HadoopUtil.delete(conf, diagOutput);
        Job job = new Job(conf, "DiagonalizeJob");
        Scan scan = new Scan();
        FileInputFormat.addInputPath(job, affInput);
        FileOutputFormat.setOutputPath(job, diagOutput);
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
        job.setJarByClass(DiagonalizeJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
    }
}