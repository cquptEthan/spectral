package edu.cqupt.spectral.serial;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.model.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.function.Functions;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 2/1/16
 * Time: 9:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class Serial {
    @Test
    public void run() throws IOException {
        Random random = new Random(System.currentTimeMillis());
        int[][] init = new int[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.COL))];
        double[][] affinity = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        double[] diagonalize = new double[Integer.valueOf(String.valueOf(Tools.ROW))];
        double[][] laplacian = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        double[][] q = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        double[][] r = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.ROW))];
        double[][] svd = new double[Integer.valueOf(String.valueOf(Tools.ROW))][Integer.valueOf(String.valueOf(Tools.K))];

        List<double []>kmeans = new ArrayList<double[]>();
        HashMap<Integer,ArrayList<Integer>> hashMap = new HashMap<>();
        for(int i = 0 ; i < Tools.X ; i ++){
            hashMap.put(i,new ArrayList<Integer>());
        }


        //init
        System.out.println("init");
        for(int i  = 0 ; i < Tools.ROW ; i++){
            for(int j  = 0 ; j < Tools.COL ; j++){
                Integer value =  random.nextInt(1000);
                init[i][j] = value;
            }
        }

        //affinity
        System.out.println("affinity");
        for(int i  = 0 ; i < Tools.ROW ; i++){
            for(int j  = 0 ; j < Tools.ROW ; j++){
                affinity[i][j] = computeSimilarity(init[i],init[j]);
            }
        }

//        diagonalize
        System.out.println("diagonalize");
        for(int i  = 0 ; i < Tools.ROW ; i++){
            for(int j  = 0 ; j < Tools.ROW ; j++){
                diagonalize[i] += affinity[i][j];
            }
        }

//        laplacian
        System.out.println("laplacian");
        for(int i  = 0 ; i < Tools.ROW ; i++){
            for(int j  = 0 ; j < Tools.ROW ; j++){
                laplacian[i][j] = affinity[i][j] - diagonalize[i];
            }
        }

//        qr
        System.out.println("qr");
        for (int x = 0 ; x < Tools.QR ; x++){

//            map
            for(int i = 0 ; i < Tools.ROW ; i ++){
                double[] aLsit = laplacian[i].clone();
                double[] bLsit = laplacian[i].clone();
                for(int k =0 ; k < i; k++){
                    double rr = 0d;
                    for(int m = 0 ; m <aLsit.length ; m ++){
                        rr += aLsit[m]*q[m][k];
                    }
                    r[k][i] = rr;
                    for(int m = 0 ; m <aLsit.length ; m ++){
                        bLsit[m] -= r[k][i]*q[m][k];
                    }
                }

                double temp = 0d ;

                for(int xx = 0 ; xx < aLsit.length ; xx++){
                    temp += Functions.SQUARE.apply(bLsit[xx]);
                }
                double s = Functions.SQRT.apply(temp);
                r[i][i] = s;
                for (int xx = 0 ; xx <aLsit.length ; xx++){
                    q[xx][i] = bLsit[xx]/s;
                }
            }


            //            reduce
            for (int i=0 ; i < Tools.ROW ; i ++){

                for (int j = 0 ; j < Tools.ROW ; j ++){
                    double temp = 0d;
                    for (int k = 0 ; k <Tools.ROW ; k ++ ){
                        temp += r[i][k]*q[k][j];
                    }
                    laplacian[i][j] = temp;
                }
            }
        }
//       sort
        ArrayList<IntDoublePairWritable> list = new ArrayList<IntDoublePairWritable>();
        for(int i = 0 ; i < laplacian.length ; i ++){
            list.add(new IntDoublePairWritable(i,laplacian[i][i]));
        }
        Collections.sort(list);

        for(int i =0 ; i < laplacian.length ; i++){
            for(int j = 0 ; j < Tools.K ; j ++){
                svd[i][j] = laplacian[i][list.get(j).getKey()];
            }
        }

//        random k
        int [] ids = Tools.randomCommon(0,Integer.valueOf(String.valueOf(Tools.ROW)),Tools.X);
        for(int i = 0 ; i < ids.length ; i ++){
            kmeans.add(svd[ids[i]]);
            System.out.println("ids[i]:  " + ids[i]);
        }
//        kemans
        for (int k = 0 ; k < Tools.KMEANS ; k ++){

            Iterator iterInit = hashMap.entrySet().iterator();
            while (iterInit.hasNext()) {
                Map.Entry entry = (Map.Entry) iterInit.next();
                ArrayList<Integer> val = (ArrayList<Integer>) entry.getValue();
                val.clear();
            }

            for(int i = 0 ; i < svd.length ; i++){
                ArrayList<IntDoublePairWritable> kmeansDiff = new ArrayList<IntDoublePairWritable>();
                for(int j = 0 ; j < kmeans.size() ; j ++){
                    IntDoublePairWritable intDoublePairWritable = new IntDoublePairWritable();
                    intDoublePairWritable.setKey(j);
                    intDoublePairWritable.setValue(diff(svd[i],kmeans.get(j)));
                    kmeansDiff.add(intDoublePairWritable);
                }
                Collections.sort(kmeansDiff);
//                Collections.reverse(kmeansDiff);
//                System.out.println(kmeansDiff.get(0).getKey());
                hashMap.get(kmeansDiff.get(0).getKey()).add(i);
            }
//            System.out.println(hashMap.toString());
            kmeans.clear();
            Iterator iter = hashMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Integer key = (Integer) entry.getKey();
                ArrayList<Integer> val = (ArrayList<Integer>) entry.getValue();

                double [] avgs = new double[Tools.K];
                for(int i = 0 ; i < Tools.K ; i ++){
                    double sum = 0d;
                    for(int j = 0 ; j < val.size() ; j++){
//                        System.out.println("fffff"+ i + "," +val.get(j));
                        sum += svd[val.get(j)][i];
                    }

                   double avg = sum/val.size();
                   avgs[i] = avg;
//                    System.out.println(i+","+avg +  " " +val.size());
                }
                kmeans.add(avgs);

            }
        }
        printResult(hashMap);

    }


    public static void  printResult(HashMap hashMap) throws IOException {
        Iterator iter = hashMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Integer key = (Integer) entry.getKey();
            ArrayList<Integer> val = (ArrayList<Integer>) entry.getValue();
            System.out.println("Num : " + key + " group:");
            for (Integer i : val){
                System.out.print(i);
                System.out.print(",");
            }
            System.out.print('\n');
        }
    }





    private double computeSimilarity(int[] a , int[]b) throws IOException {
        double squareSum  = 0d ;
        double similarity = 0d;
        for (int i = 0 ; i < a.length ; i ++){
            double square = Functions.SQUARE.apply( Double.valueOf(a[i]-b[i]));
            squareSum += square;
        }
        similarity = Math.exp(-squareSum/(2*Tools.OMG*Tools.OMG));
        return similarity;
    }

    private double diff(double[] xs , double[] ys ){
        double diff = 0d;
        double sum = 0d;
        for(int i =0 ; i < ys.length; i++){
            sum += Functions.SQUARE.apply(xs[i] - ys[i]);
        }
        return Functions.SQRT.apply(sum);
    }


}
