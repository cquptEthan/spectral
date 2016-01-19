package edu.cqupt.spectral.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/18/16
 * Time: 4:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class Tools {
    public static Vector load(Configuration conf) throws IOException {
        Path[] files = HadoopUtil.getCachedFiles(conf);

        if (files.length != 1) {
            throw new IOException("Cannot read Frequency list from Distributed Cache (" + files.length + ')');
        }

        return load(conf, files[0]);
    }

    public static Vector load(Configuration conf, Path input) throws IOException {
        try (SequenceFileValueIterator<VectorWritable> iterator =
                     new SequenceFileValueIterator<>(input, true, conf)){
            return iterator.next().get();
        }
    }
}
