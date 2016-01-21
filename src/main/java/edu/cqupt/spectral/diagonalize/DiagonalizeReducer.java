package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/18/16
 * Time: 2:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class DiagonalizeReducer
        extends TableReducer<IntWritable,DoubleWritable,ImmutableBytesWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
         Put put = new Put(key.toString().getBytes());
         put.add(Tools.DIAGONALIZE_FAMILY_NAME.getBytes(),Tools.DIAGONALIZE_VALUE_NAME.getBytes(),values.iterator().next().toString().getBytes());
        context.write(null,put);
    }
}
