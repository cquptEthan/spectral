package edu.cqupt.spectral.input;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/20/16
 * Time: 9:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class InitInputJob {
    public static void runJob(Path input, Path output)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        HadoopUtil.delete(conf, output);

//        conf.setInt(Keys.AFFINITY_DIMENSIONS, rows);
        Job job = new Job(conf, "init");
//        HTable affinityTable = new HTable(HBaseConfiguration.create(), Tools.AFFINITY_TABLE_NAME);
        Scan scan = new Scan();
        job.setJarByClass(InitInputJob.class);
//        job.setMap
        FileOutputFormat.setOutputPath(job,output);

        TableMapReduceUtil.initTableMapperJob(Tools.AFFINITY_TABLE_NAME, scan, InitInputMapper.class,null,null
                , job);
//        TableMapReduceUtil.initTableReducerJob(
//                Tools.AFFINITY_TABLE_NAME,        // output table
//                InitInputReduce.class,    // reducer class
//                job);
        job.setNumReduceTasks(0);
        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }
    }
}
