package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.input.InitInputMapper;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import edu.cqupt.spectral.qr.QrMapper;
import edu.cqupt.spectral.qr.QrReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
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
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "scmhadoop-1");
//        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if(admin.tableExists(Tools.KMEANS_TABLE_NAME)){
            admin.disableTable(Tools.KMEANS_TABLE_NAME);
            admin.deleteTable(Tools.KMEANS_TABLE_NAME);
        }
        HTableDescriptor kmeansTableDesc = new HTableDescriptor(Tools.KMEANS_TABLE_NAME);
        HColumnDescriptor kmeansColumnDescriptor = new HColumnDescriptor(Tools.KMEANS_FAMILY_NAME) ;
        kmeansColumnDescriptor.setMaxVersions(1);
        kmeansTableDesc.addFamily(kmeansColumnDescriptor);
        admin.createTable(kmeansTableDesc);

        HTable hTable = new HTable(configuration,Tools.KMEANS_TABLE_NAME);

      int[] xCenter =  Tools.randomCommon(0,Tools.K,Tools.X);
        for(int i = 0 ; i < xCenter.length ; i ++){
            Put put = new Put(String.valueOf(xCenter).getBytes());
            put.add(Tools.KMEANS_FAMILY_NAME.getBytes(),Tools.KMEANS_VALUE_NAME.getBytes(),String.valueOf(xCenter).getBytes());
            hTable.put(put);
        }

        for (int i = 0 ; i < k ; i ++){
            runJob();
        }
    }
    public static void runJob()
            throws IOException, InterruptedException, ClassNotFoundException {


        String sourceTable = Tools.SVD_TABLE_NAME;
        String targetTable = Tools.KMEANS_TABLE_NAME;
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "KmeansJob");
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,        // input table
                scan,               // Scan instance to control CF and attribute selection
                KMeansMapper.class,     // mapper class
                IntWritable.class,         // mapper output key
                IntDoublePairWritable.class,  // mapper output value
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
