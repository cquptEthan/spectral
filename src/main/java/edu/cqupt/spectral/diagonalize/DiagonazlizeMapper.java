package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/18/16
 * Time: 1:35 PM
 * To change this template use File | Settings | File Templates.
 */


public  class DiagonazlizeMapper
        extends Mapper<IntWritable, VectorWritable, NullWritable, IntDoublePairWritable> {
    @Override
    protected void map(IntWritable key, VectorWritable row, Context context)
            throws IOException, InterruptedException {
        //求和
        IntDoublePairWritable store = new IntDoublePairWritable(key.get(), row.get().zSum());
        context.write(NullWritable.get(), store);
    }
}