package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.input.InitInputMapper;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import edu.cqupt.spectral.qr.QrMapper;
import edu.cqupt.spectral.qr.QrReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/21/16
 * Time: 9:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class KMeansJob {
    private static HTable svdTable;
    public static void iter( Configuration conf , Integer k) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration configuration = HBaseConfiguration.create();
        svdTable = new HTable(configuration, Tools.SVD_TABLE_NAME);
//        configuration.set("hbase.zookeeper.quorum", "scmhadoop-1");
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
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

      int[] xCenter =  Tools.randomCommon(0,Integer.valueOf(String.valueOf(Tools.ROW)),Tools.X);


        for(int i = 0 ; i < xCenter.length ; i ++){
            for(int j = 0 ; j < Tools.K ; j++){
                Put put = new Put(String.valueOf(i).getBytes());
                put.add(Tools.KMEANS_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),String.valueOf(Double.valueOf(getSvd(xCenter[i],j))).getBytes());
                hTable.put(put);
            }
        }

        for (int i = 0 ; i < k ; i ++){
            runJob(conf);
        }
    }
    public static void runJob( Configuration conf)
            throws IOException, InterruptedException, ClassNotFoundException {


        String sourceTable = Tools.SVD_TABLE_NAME;
        String targetTable = Tools.SVD_TABLE_NAME;
        conf.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        Job job = new Job(conf, "KmeansJob");
        Scan scan = new Scan();
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

    private static double getSvd(int i,int j) throws IOException {
        Get get = new Get(String.valueOf(i).getBytes());
        get.addColumn(Tools.SVD_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes());
        Result secondRes = svdTable.get(get);
        List<Cell> secondCells = secondRes.listCells();
        if(secondCells != null){
            return  Double.valueOf(new String(CellUtil.cloneValue(secondCells.get(0))));
        }else {
            return 0d;
        }
    }
}
