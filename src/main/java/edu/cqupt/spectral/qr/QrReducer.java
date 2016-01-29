package edu.cqupt.spectral.qr;

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
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/26/16
 * Time: 10:19 AM
 * To change this template use File | Settings | File Templates.
 */
public class QrReducer extends TableReducer<IntWritable,IntWritable,ImmutableBytesWritable> {
    private HTable qTable;
    private HTable rTable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        qTable = new HTable(configuration, Tools.Q_TABLE_NAME);
        rTable = new HTable(configuration, Tools.R_TABLE_NAME);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> integerArrayList = new ArrayList<Integer>();
        for(IntWritable intWritable : values){
            integerArrayList.add(intWritable.get());
        }
        int i = key.get();
         for (int j = 0 ; j< integerArrayList.size() ;j++){
             double temp = 0;
             for (int k =0 ; k < integerArrayList.size() ; k++){
                 temp += getR(i,k)*getQ(k,j);
             }
             Put put = new Put(String.valueOf(i).getBytes());
             put.add(Tools.LAPLACIAN_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),String.valueOf(temp).getBytes());
             context.write(null,put);
         }
    }

    private Double getQ(int i , int j) throws IOException {
        Get get = new Get(String.valueOf(i).getBytes());
        get.addColumn(Tools.Q_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes());
        Result secondRes = qTable.get(get);
        List<Cell> secondCells = secondRes.listCells();
        if(secondCells != null){
            return  Double.valueOf(new String(CellUtil.cloneValue(secondCells.get(0))));
        }else {
            return 0d;
        }
    }

    private Double getR(int i , int j) throws IOException {
        Get get = new Get(String.valueOf(i).getBytes());
        get.addColumn(Tools.R_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes());
        Result secondRes = rTable.get(get);
        List<Cell> secondCells = secondRes.listCells();
        if(secondCells != null){
            return  Double.valueOf(new String(CellUtil.cloneValue(secondCells.get(0))));
        }else {
            return 0d;
        }
    }

    private void putQ(int i , int j ,double value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(String.valueOf(i).getBytes());
        put.add(Tools.Q_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),String.valueOf(value).getBytes());
        qTable.put(put);
    }

    private void putR(int i , int j ,double value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(String.valueOf(i).getBytes());
        put.add(Tools.R_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),String.valueOf(value).getBytes());
        rTable.put(put);
    }
}
