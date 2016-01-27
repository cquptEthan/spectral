package edu.cqupt.spectral.kmeans;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/15/16
 * Time: 11:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class KMeansReducer extends TableReducer<IntWritable,IntDoublePairWritable,ImmutableBytesWritable>{

    @Override
    protected void reduce(IntWritable key, Iterable<IntDoublePairWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<IntDoublePairWritable> intDoublePairWritableArrayList = new ArrayList<IntDoublePairWritable>();
        for(IntDoublePairWritable intDoublePairWritable : values){
            intDoublePairWritableArrayList.add(intDoublePairWritable);
        }
        Collections.sort(intDoublePairWritableArrayList);
                Put put = new Put(String.valueOf(intDoublePairWritableArrayList.get(0).getKey()).getBytes());
                put.add(Tools.KMEANS_FAMILY_NAME.getBytes(),Tools.KMEANS_VALUE_NAME.getBytes(),String.valueOf(intDoublePairWritableArrayList.get(0).getValue()).getBytes());
                context.write(null,put);
        }
    }
