package edu.cqupt.spectral.input;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/20/16
 * Time: 9:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class InitInputMapper extends TableMapper<NullWritable,NullWritable> {
    private HTable initTable;
    private HTable affinityTable;
    private double omg = 1000d;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "scmhadoop-1");
        initTable = new HTable(configuration, Tools.INIT_TABLE_NAME);
        affinityTable = new HTable(configuration, Tools.AFFINITY_TABLE_NAME);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//       System.out.println(key);
//       System.out.println(value);
        List<Put> puts = new ArrayList<Put>();
        List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                Integer secKey = Integer.valueOf(new String(CellUtil.cloneQualifier(cell)));
                Get fisrtGet = new Get(key.get());
                Get secondGet = new Get(secKey.toString().getBytes());
                Double sim = computeSimilarity(fisrtGet,secondGet);
//                context.write(new IntWritable(secKey),new BytesWritable(sim.toString().getBytes()));
                Put put = new Put(key.get());
                put.add(Tools.AFFINITY_FAMILY_NAME.getBytes(),secKey.toString().getBytes(),sim.toString().getBytes());
                puts.add(put);
            }
        affinityTable.put(puts);
//        context.write(null,null);
    }
    private double computeSimilarity(Get firstGet , Get secondGet) throws IOException {
        Result firstRes = initTable.get(firstGet);
        Result secondRes = initTable.get(secondGet);
        double similarity = 0d;
        List<Cell> firstCells = firstRes.listCells();
        List<Cell> secondCells = secondRes.listCells();
        double squareSum  = 0d ;

        for (int i = 0 ; i < firstCells.size() ; i ++){
            double square = Functions.SQUARE.apply(
                    Double.valueOf(new String(CellUtil.cloneValue(firstCells.get(i)))) -
                          Double.valueOf(new String(CellUtil.cloneValue(secondCells.get(i)))));
            squareSum += square;
        }
        similarity = Math.exp(-squareSum/(2*omg*omg));
        return similarity;
    }
}

