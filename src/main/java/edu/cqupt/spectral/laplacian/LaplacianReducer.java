package edu.cqupt.spectral.laplacian;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/20/16
 * Time: 6:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class LaplacianReducer extends TableReducer<IntWritable,IntDoublePairWritable,ImmutableBytesWritable> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Tools.setConf(context.getConfiguration());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntDoublePairWritable> values, Context context) throws IOException, InterruptedException {

        for(IntDoublePairWritable intDoublePairWritable : values){
            Put put = new Put(String.valueOf(key.get()).getBytes());
            put.add(Tools.LAPLACIAN_FAMILY_NAME.getBytes(),String.valueOf(intDoublePairWritable.getKey()).getBytes(),String.valueOf(intDoublePairWritable.getValue()).getBytes());
            context.write(null,put);
        }

    }
}
