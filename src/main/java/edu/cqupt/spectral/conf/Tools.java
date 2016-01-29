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
//    public  static  final String ZOOKEEPER ="localhost";
//    public  static  final String ZOOKEEPER ="scmhadoop-1,scmhadoop-2,scmhadoop-3";
    public  static  final String ZOOKEEPER ="cqupt-01,cqupt-02,cqupt-03";
    public  static  final String INIT_TABLE_NAME = "spectral_init";
    public  static  final String AFFINITY_TABLE_NAME = "spectral_affinity";
    public  static  final String DIAGONALIZE_TABLE_NAME = "spectral_diagonalize";
    public  static  final String LAPLACIAN_TABLE_NAME = "spectral_laplacian";
    public  static  final String SVD_TABLE_NAME = "spectral_svd";
    public  static  final String Q_TABLE_NAME = "spectral_q";
    public  static  final String R_TABLE_NAME = "spectral_r";
    public  static  final String KMEANS_TABLE_NAME = "spectral_kmeans";
    public  static  final String KMEANS_FAMILY_NAME = "col";
    public  static  final String KMEANS_VALUE_NAME = "col";
    public  static  final String SVD_FAMILY_NAME = "col";
    public  static  final String Q_FAMILY_NAME = "col";
    public  static  final String R_FAMILY_NAME = "col";
    public  static  final String SVD_VALUE_NAME = "group";
    public  static  final String LAPLACIAN_FAMILY_NAME = "col";
    public  static  final String INIT_FAMILY_NAME = "col";
    public  static  final String AFFINITY_FAMILY_NAME = "col";
    public  static  final String DIAGONALIZE_FAMILY_NAME = "col";
    public  static  final String DIAGONALIZE_VALUE_NAME = "value";

    public  static  Long ROW= 20L;      //100M
    public  static    Long COL = 150L;
    public  static    int K = 8;
    public  static    int X = 3;
    public  static    double OMG = 10000d;
    public  static    int QR = 10;
    public  static    int KMEANS = 10;

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

    public static int[] randomCommon(int min, int max, int n){
        if (n > (max - min + 1) || max < min) {
            return null;
        }
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            for (int j = 0; j < n; j++) {
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }
}
