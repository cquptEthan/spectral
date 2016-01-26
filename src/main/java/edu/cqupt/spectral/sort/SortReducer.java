package edu.cqupt.spectral.sort;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/26/16
 * Time: 5:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class SortReducer extends TableReducer<IntWritable,IntDoublePairWritable,ImmutableBytesWritable> {
    private int k;
    private HTable qTable;
    private HTable rTable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        qTable = new HTable(configuration, Tools.Q_TABLE_NAME);
        rTable = new HTable(configuration, Tools.R_TABLE_NAME);
        k = 5;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntDoublePairWritable> values, Context context) throws IOException, InterruptedException {
//        double [] doubles = new double[key.get()];
//        doubleArrayList.sort();
        ArrayList<IntDoublePairWritable>  intDoublePairWritableArrayList = new ArrayList<IntDoublePairWritable>();
        for(IntDoublePairWritable intDoublePairWritable : values){
            intDoublePairWritableArrayList.add(intDoublePairWritable);
        }
        Collections.sort(intDoublePairWritableArrayList);

        for(int i = 0 ; i < k; i++){
            int x = intDoublePairWritableArrayList.get(i).getKey();
            for(int j = 0 ; j < intDoublePairWritableArrayList.size() ; j ++){
                Put put = new Put(String.valueOf(i).getBytes());
                put.add(Tools.SVD_FAMILY_NAME.getBytes(),String.valueOf(j).getBytes(),getQ(x,j).toString().getBytes());
                context.write(null,put);
            }
        }
//        intDoublePairWritableArrayList.sort(Comparator.<IntDoublePairWritable>naturalOrder());

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
}
