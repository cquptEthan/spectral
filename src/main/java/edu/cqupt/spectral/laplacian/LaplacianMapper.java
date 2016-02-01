package edu.cqupt.spectral.laplacian;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/20/16
 * Time: 6:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class LaplacianMapper extends TableMapper<IntWritable,IntDoublePairWritable> {
    private HTable diagonalizeTable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Tools.setConf(context.getConfiguration());
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", Tools.ZOOKEEPER);
        diagonalizeTable = new HTable(configuration, Tools.DIAGONALIZE_TABLE_NAME);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        IntDoublePairWritable intDoublePairWritable = new IntDoublePairWritable();
        Get fisrtGet = new Get(key.get());
        Result firstRes = diagonalizeTable.get(fisrtGet);
        Double d = Double.valueOf(new String(CellUtil.cloneValue(firstRes.listCells().get(0))));

        int i = Integer.valueOf(new String(key.get())) ;
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            double values = 0d;
            int j = Integer.valueOf(new String(CellUtil.cloneQualifier(cell)));
            if(i != j){
            values = - Double.valueOf(new String(CellUtil.cloneValue(cell)));
            }else{
            values = d - Double.valueOf(new String(CellUtil.cloneValue(cell)));
            }
            intDoublePairWritable.setKey(j);
            intDoublePairWritable.setValue(values);
            context.write(new IntWritable(i), intDoublePairWritable);
        }
    }
}
