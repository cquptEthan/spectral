package edu.cqupt.spectral.input;

import edu.cqupt.spectral.conf.Tools;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/25/16
 * Time: 5:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class InitInputReduce extends TableReducer<IntWritable,BytesWritable,ImmutableBytesWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value : values){
        Put put = new Put(key.toString().getBytes());
        put.add(Tools.AFFINITY_FAMILY_NAME.getBytes(),key.toString().getBytes(),value.getBytes());
        context.write(null,put);
        }
    }
}
