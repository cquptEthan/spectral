package edu.cqupt.spectral.sort;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import edu.cqupt.spectral.qr.QrMapper;
import edu.cqupt.spectral.qr.QrReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/26/16
 * Time: 5:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class SortJob {

    public static void runJob(Configuration conf)
            throws IOException, ClassNotFoundException, InterruptedException {
        String sourceTable = Tools.LAPLACIAN_TABLE_NAME;
        String targetTable = Tools.SVD_TABLE_NAME;
        // set up all the job tasks
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "Sort");
        Scan scan = new Scan();
//        FileInputFormat.addInputPath(job, affInput);
//        FileOutputFormat.setOutputPath(job, diagOutput);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                SortMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                IntDoublePairWritable.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                SortReducer.class,    // reducer class
                job);
        job.setJarByClass(SortJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
    }
}
