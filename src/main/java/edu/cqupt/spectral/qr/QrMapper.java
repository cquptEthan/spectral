package edu.cqupt.spectral.qr;

import edu.cqupt.spectral.conf.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/25/16
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class QrMapper extends TableMapper<IntWritable,IntWritable> {
    private HTable qTable;
    private HTable rTable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Tools.setConf(context.getConfiguration());
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        qTable = new HTable(configuration, Tools.Q_TABLE_NAME);
        rTable = new HTable(configuration, Tools.R_TABLE_NAME);

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

//        QR
        List<Cell> cells = value.listCells();
//        cells.size()
        double [] aList = new double[cells.size()];
        double [] bList = new double[cells.size()];

        double [][]qList  = getQList();
        double [][]rList  = getRList();


//        ArrayList<Double> aList = new ArrayList(cells.size());
//        ArrayList<Double> bList = new ArrayList(cells.size());
        int i = Integer.valueOf(new String(key.get())) ;
        for (Cell cell : cells) {
            int j = Integer.valueOf(new String(CellUtil.cloneQualifier(cell)));
            aList[j] =  Double.valueOf(new String(CellUtil.cloneValue(cell)));
            bList[j] =  Double.valueOf(new String(CellUtil.cloneValue(cell)));
//            aList.add(j,Double.valueOf(new String(CellUtil.cloneValue(cell))));
//            bList.add(j,Double.valueOf(new String(CellUtil.cloneValue(cell))));
        }

        for(int k = 0 ; k <= i ; k++){
            double r = 0d;
            for(int m = 0 ; m < aList.length ; m ++){
//                r+=aList[m]*getQ(m,k);
                r+=aList[m]* qList[m][k];
            }
            putR(k,i,r);
            for (int m = 0 ;m < aList.length ;m ++){
                bList[m] -= rList[k][i]*qList[m][k];
//                bList[m] -= getR(k,i)*getQ(m,k);
//                bList.set(m,bList.get(m) -getR(k,i)*getQ(m,k));
            }
        }

        double temp = 0d;

        for(int x = 0 ; x < aList.length ; x++){
            temp += Functions.SQUARE.apply(bList[x]);
        }
        double s = Functions.SQRT.apply(temp);
        putR(i,i,s);
        for (int x = 0 ; x <aList.length ; x++){
            putQ(x,i,bList[x]/s);
            context.write(new IntWritable(i),new IntWritable(x));
        }
//         cheng
//         for(int ii = 0 ; ii < aList.size() ; ii ++){
//             for(jj = 0 ; )
//         }


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

    private double[][] getQList() throws IOException {
        ResultScanner rs = null;
        Scan scan = new Scan();
        rs =  qTable.getScanner(scan);
        double[][] ids = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            if(cells != null ){
                for (Cell cell : cells) {
                    ids[Integer.valueOf(new String(CellUtil.cloneRow(cell)))][Integer.valueOf(new String(CellUtil.cloneQualifier(cell)))] =  Double.valueOf(new String(CellUtil.cloneValue(cell)));
                }

            }
        }
        return ids;
    }
    private double[][] getRList() throws IOException {
        ResultScanner rs = null;
        Scan scan = new Scan();
        rs =  rTable.getScanner(scan);
        double[][] ids = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            if(cells != null ){

                for (Cell cell : cells) {
                    ids[Integer.valueOf(new String(CellUtil.cloneRow(cell)))][Integer.valueOf(new String(CellUtil.cloneQualifier(cell)))] =  Double.valueOf(new String(CellUtil.cloneValue(cell)));
                }

            }
        }
        return ids;
    }

}
