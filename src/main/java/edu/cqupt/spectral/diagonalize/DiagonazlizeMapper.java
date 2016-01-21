package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/18/16
 * Time: 1:35 PM
 * To change this template use File | Settings | File Templates.
 */


public  class DiagonazlizeMapper
        extends TableMapper<IntWritable,DoubleWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        DoubleWritable doubleWritable = new DoubleWritable();
        double sum = 0d;
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            sum += Double.valueOf(new String(CellUtil.cloneValue(cell)));
        }
        doubleWritable.set(sum);
        context.write(new IntWritable(Integer.valueOf(new String(key.get()))),doubleWritable);
    }
}