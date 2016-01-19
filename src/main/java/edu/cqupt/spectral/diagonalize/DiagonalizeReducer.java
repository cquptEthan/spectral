package edu.cqupt.spectral.diagonalize;

import edu.cqupt.spectral.model.IntDoublePairWritable;
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
        extends Reducer<NullWritable, IntDoublePairWritable, NullWritable, VectorWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<IntDoublePairWritable> values,
                          Context context) throws IOException, InterruptedException {
        // create the return vector
        Vector retval = new DenseVector(context.getConfiguration().getInt("edu.cqupt.spectral.dimensions",Integer.MAX_VALUE));
        // put everything in its correct spot
        for (IntDoublePairWritable e : values) {
            retval.setQuick(e.getKey(), e.getValue());
        }
        // write it out
        context.write(key, new VectorWritable(retval));
    }
}
