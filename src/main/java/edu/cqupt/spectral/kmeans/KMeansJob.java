package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.input.InitInputMapper;
import edu.cqupt.spectral.qr.QrMapper;
import edu.cqupt.spectral.qr.QrReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/21/16
 * Time: 9:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class KMeansJob {
    public static void iter(Integer k) throws InterruptedException, IOException, ClassNotFoundException {
        for (int i = 0 ; i < k ; i ++){
            runJob();
        }
    }
    public static void runJob()
            throws IOException, InterruptedException, ClassNotFoundException {
        String sourceTable = Tools.SVD_TABLE_NAME;
        String targetTable = Tools.SVD_TABLE_NAME;
        // set up all the job tasks
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "KmeansJob");
        Scan scan = new Scan();
//        FileInputFormat.addInputPath(job, affInput);
//        FileOutputFormat.setOutputPath(job, diagOutput);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                KMeansMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                IntWritable.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                KMeansReducer.class,    // reducer class
                job);
        job.setJarByClass(KMeansJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }
    }
}
