package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/15/16
 * Time: 11:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class KMeansMapper extends TableMapper<IntWritable,IntDoublePairWritable>{

    private HTable kTable;
    private HTable svdTable;
    private List<Integer> kmeansPoint;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        svdTable = new HTable(configuration, Tools.SVD_TABLE_NAME);
        kTable = new HTable(configuration, Tools.KMEANS_TABLE_NAME);
        kmeansPoint   = getKmeans();

    }
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        List<Cell> cells = value.listCells();
        ArrayList<IntDoublePairWritable>  diffList = new ArrayList<IntDoublePairWritable>();
        double [] aList = new double[cells.size()];
        int i = Integer.valueOf(new String(key.get())) ;
        for (Cell cell : cells) {
            int j = Integer.valueOf(new String(CellUtil.cloneQualifier(cell)));
            aList[j] =  Double.valueOf(new String(CellUtil.cloneValue(cell)));
        }
        for (int x = 0 ; x < kmeansPoint.size() ; x ++){
            IntDoublePairWritable intDoublePairWritable = new IntDoublePairWritable();
            double [] bList  = new double[cells.size()];
            for(int n = 0 ; n < cells.size() ; n ++){
                bList[n] = getSvd(kmeansPoint.get(x),n);
            }
            intDoublePairWritable.setKey(x);
            intDoublePairWritable.setValue(diff(aList,bList));
            diffList.add(intDoublePairWritable);
        }
        Collections.sort(diffList);
        context.write(new IntWritable(0),diffList.get(0));
    }

    private double diff(double[] xs , double[] ys ){
        double diff = 0d;
        double sum = 0d;
        for(int i =0 ; i < xs.length; i++){
            sum += Functions.SQUARE.apply(xs[i] - ys[i]);
        }
        return Functions.SQRT.apply(sum);
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

    private ArrayList<Integer> getKmeans () throws IOException {
        ArrayList<Integer> ids = new ArrayList<Integer>();
        ResultScanner rs = null;
        Scan scan = new Scan();
        rs =  kTable.getScanner(scan);
//        ids.toArray();
        //统计记录条数
        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            if(cells != null ){
                for (Cell cell : cells) {
                    if (new String(CellUtil.cloneQualifier(cell)).equals(Tools.KMEANS_VALUE_NAME)) {
                        ids.add(Integer.valueOf(new String(CellUtil.cloneValue(cell))));
                    }
                }
            }
        }
        return ids;
    }
}
