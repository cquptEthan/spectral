package edu.cqupt.spectral.qr;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeReducer;
import edu.cqupt.spectral.diagonalize.DiagonazlizeMapper;
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
 * Date: 1/25/16
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class QrJob {
    public static void iter(Integer k) throws InterruptedException, IOException, ClassNotFoundException {
        for (int i = 0 ; i < k ; i ++){
            runJob();
        }
    }

    public static void runJob()
            throws IOException, ClassNotFoundException, InterruptedException {
        String sourceTable = Tools.LAPLACIAN_TABLE_NAME;
        String targetTable = Tools.LAPLACIAN_TABLE_NAME;
        // set up all the job tasks
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "QrJob");
        Scan scan = new Scan();
//        FileInputFormat.addInputPath(job, affInput);
//        FileOutputFormat.setOutputPath(job, diagOutput);
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                QrMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                IntWritable.class,  // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,        // output table
                QrReducer.class,    // reducer class
                job);
        job.setJarByClass(QrJob.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        // read the results back from the path
    }
}
