package edu.cqupt.spectral.sort;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
 * Date: 1/26/16
 * Time: 5:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class SortMapper extends TableMapper<IntWritable,IntDoublePairWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Tools.setConf(context.getConfiguration());
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        int i = Integer.valueOf(new String(key.get())) ;
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            int j = Integer.valueOf(new String(CellUtil.cloneQualifier(cell)));
            if(j==i){
                IntDoublePairWritable intDoublePairWritable = new IntDoublePairWritable();
                intDoublePairWritable.setKey(i);
                intDoublePairWritable.setValue(Double.valueOf(new String(CellUtil.cloneValue(cell))));
                context.write(new IntWritable(cells.size()),intDoublePairWritable);
            }
        }
    }
}
