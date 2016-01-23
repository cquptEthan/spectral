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
    public  static  final String INIT_TABLE_NAME = "spectral_init";
    public  static  final String AFFINITY_TABLE_NAME = "spectral_affinity";
    public  static  final String DIAGONALIZE_TABLE_NAME = "spectral_diagonalize";
    public  static  final String LAPLACIAN_TABLE_NAME = "spectral_laplacian";
    public  static  final String SVD_TABLE_NAME = "spectral_svd";
    public  static  final String SVD_FAMILY_NAME = "col";
    public  static  final String SVD_VALUE_NAME = "value";
    public  static  final String LAPLACIAN_FAMILY_NAME = "col";
    public  static  final String INIT_FAMILY_NAME = "col";
    public  static  final String AFFINITY_FAMILY_NAME = "col";
    public  static  final String DIAGONALIZE_FAMILY_NAME = "col";
    public  static  final String DIAGONALIZE_VALUE_NAME = "value";

    public  static final Long ROW= 22000L;      //100M
    public  static  final  Long COL = 150L;

//    public  static final Long ROW= 22L;
//    public  static  final  Long COL = 15L;


    public static Vector load(Configuration conf) throws IOException {
        Path[] files = HadoopUtil.getCachedFiles(conf);
//        Integer.class
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
