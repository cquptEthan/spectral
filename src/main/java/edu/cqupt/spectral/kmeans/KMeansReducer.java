package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/15/16
 * Time: 11:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class KMeansReducer extends TableReducer<IntWritable,IntWritable,ImmutableBytesWritable>{
    private HTable kTable;
    private HTable svdTable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        kTable = new HTable(configuration, Tools.KMEANS_TABLE_NAME);
        svdTable = new HTable(configuration, Tools.SVD_TABLE_NAME);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        ArrayList<IntDoublePairWritable> intDoublePairWritableArrayList = new ArrayList<IntDoublePairWritable>();
       ArrayList<Integer> idList = new ArrayList<Integer>() ;
        for(IntWritable intDoublePairWritable : values){
//            intDoublePairWritableArrayList.add(intDoublePairWritable);
            idList.add(intDoublePairWritable.get());
            Put put = new Put(String.valueOf(intDoublePairWritable.get()).getBytes());
            put.add(Tools.SVD_FAMILY_NAME.getBytes(),Tools.SVD_VALUE_NAME.getBytes(),String.valueOf(key.get()).getBytes());
            context.write(null,put);
        }
//        Collections.sort(intDoublePairWritableArrayList);
//                Put put = new Put(String.valueOf(intDoublePairWritableArrayList.get(0).getKey()).getBytes());
//                put.add(Tools.SVD_FAMILY_NAME.getBytes(),Tools.SVD_VALUE_NAME.getBytes(),String.valueOf(intDoublePairWritableArrayList.get(0).getKey()).getBytes());
//                kTable.put();
//                svdTable.put(put);


        for(int i =0 ; i < Tools.ROW ; i ++){
        double sum = 0d;
           for (int j = 0 ; j < idList.size() ; j ++){
               sum += getSvd(j,i);
           }
            double avg = sum/idList.size();
            putK(key.get(),i,avg);
        }
        }

    private void putK(int i , int j ,double value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(String.valueOf(i).getBytes());
        put.add(Tools.KMEANS_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),String.valueOf(value).getBytes());
        kTable.put(put);
    }

    private double getSvd(int i,int j) throws IOException {
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
