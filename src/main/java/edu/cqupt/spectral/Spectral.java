package edu.cqupt.spectral;

import java.io.IOException;
import java.util.*;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeJob;
import edu.cqupt.spectral.input.InitInputJob;
import edu.cqupt.spectral.kmeans.KMeansJob;
import edu.cqupt.spectral.laplacian.LaplacianJob;
import edu.cqupt.spectral.qr.QrJob;
import edu.cqupt.spectral.sort.SortJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.mahout.common.HadoopUtil;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/15/16
 * Time: 10:19 AM
 * To change this template use File | Settings | File Templates.
 */
public class Spectral {
    public static void initHbase() throws IOException {
        Long row = Tools.ROW;
        Long col = Tools.COL;
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(configuration);

        if(admin.tableExists(Tools.INIT_TABLE_NAME)){
            admin.disableTable(Tools.INIT_TABLE_NAME);
            admin.deleteTable(Tools.INIT_TABLE_NAME);
        }

        if(admin.tableExists(Tools.AFFINITY_TABLE_NAME)){
            admin.disableTable(Tools.AFFINITY_TABLE_NAME);
            admin.deleteTable(Tools.AFFINITY_TABLE_NAME);
        }

        if(admin.tableExists(Tools.DIAGONALIZE_TABLE_NAME)){
            admin.disableTable(Tools.DIAGONALIZE_TABLE_NAME);
            admin.deleteTable(Tools.DIAGONALIZE_TABLE_NAME);
        }

        if(admin.tableExists(Tools.LAPLACIAN_TABLE_NAME)){
            admin.disableTable(Tools.LAPLACIAN_TABLE_NAME);
            admin.deleteTable(Tools.LAPLACIAN_TABLE_NAME);
        }

        if(admin.tableExists(Tools.SVD_TABLE_NAME)){
            admin.disableTable(Tools.SVD_TABLE_NAME);
            admin.deleteTable(Tools.SVD_TABLE_NAME);
        }

        if(admin.tableExists(Tools.Q_TABLE_NAME)){
            admin.disableTable(Tools.Q_TABLE_NAME);
            admin.deleteTable(Tools.Q_TABLE_NAME);
        }

        if(admin.tableExists(Tools.R_TABLE_NAME)){
            admin.disableTable(Tools.R_TABLE_NAME);
            admin.deleteTable(Tools.R_TABLE_NAME);
        }

        if(admin.tableExists(Tools.KMEANS_TABLE_NAME)){
            admin.disableTable(Tools.KMEANS_TABLE_NAME);
            admin.deleteTable(Tools.KMEANS_TABLE_NAME);
        }

        HTableDescriptor initTableDesc = new HTableDescriptor(Tools.INIT_TABLE_NAME);
        initTableDesc.addFamily(new HColumnDescriptor(Tools.INIT_FAMILY_NAME));
        initTableDesc.setMaxFileSize(  Tools.HTABLE );
        admin.createTable(initTableDesc);
        Random random = new Random(System.currentTimeMillis());
        HTable initTable = new HTable(configuration,Tools.INIT_TABLE_NAME);


        List<Put> puts = new ArrayList<Put>();
        for(Long i  = 0L ; i < row ; i++){
            Put put = new Put(i.toString().getBytes());
            for(Long j  = 0L ; j < col ; j++){
                Integer value =  random.nextInt(1000);
                put.add(Tools.INIT_FAMILY_NAME.getBytes(),j.toString().getBytes(),value.toString().getBytes());
            }
            puts.add(put);
            if(puts.size() > 1000){
                initTable.put(puts);
                puts.clear();
            }
        }
        initTable.put(puts);

        HTableDescriptor affinityTableDesc = new HTableDescriptor(Tools.AFFINITY_TABLE_NAME);
        HColumnDescriptor affinityColumnDescriptor = new HColumnDescriptor(Tools.AFFINITY_FAMILY_NAME) ;
        affinityColumnDescriptor.setMaxVersions(1);
        affinityTableDesc.addFamily(affinityColumnDescriptor);
        affinityTableDesc.setMaxFileSize( Tools.HTABLE);
        admin.createTable(affinityTableDesc);

        HTable affinityTable = new HTable(configuration,Tools.AFFINITY_TABLE_NAME);

        List<Put> affinityPuts = new ArrayList<Put>();
        for(Long i  = 0L ; i < row ; i++){
            Put put = new Put(i.toString().getBytes());
            put.setWriteToWAL(false);
            for(Long j  = 0L ; j < row ; j++){
                put.add(Tools.AFFINITY_FAMILY_NAME.getBytes(),j.toString().getBytes(),"0".getBytes());
            }
            affinityPuts.add(put);
            if(affinityPuts.size() > 100){
                affinityTable.put(affinityPuts);
                affinityPuts.clear();
            }
        }
        affinityTable.put(affinityPuts);

        HTableDescriptor diagonalizeTableDesc = new HTableDescriptor(Tools.DIAGONALIZE_TABLE_NAME);
        diagonalizeTableDesc.addFamily(new HColumnDescriptor(Tools.DIAGONALIZE_FAMILY_NAME));
        admin.createTable(diagonalizeTableDesc);
        HTable diagonalizeTable = new HTable(configuration,Tools.DIAGONALIZE_TABLE_NAME);

        List<Put> diagonalizePuts = new ArrayList<Put>();
        for(Long i  = 0L ; i < row ; i++){
            Put put = new Put(i.toString().getBytes());
            put.setWriteToWAL(false);
            put.add(Tools.DIAGONALIZE_FAMILY_NAME.getBytes(),Tools.DIAGONALIZE_VALUE_NAME.toString().getBytes(),"0".getBytes());
            diagonalizePuts.add(put);
            if(diagonalizePuts.size() > 100){
                diagonalizeTable.put(diagonalizePuts);
                diagonalizePuts.clear();
            }
        }
        diagonalizeTable.put(diagonalizePuts);

        HTableDescriptor SVDTableDesc = new HTableDescriptor(Tools.SVD_TABLE_NAME);
        SVDTableDesc.addFamily(new HColumnDescriptor(Tools.SVD_FAMILY_NAME));
        SVDTableDesc.setMaxFileSize( Tools.HTABLE);
        admin.createTable(SVDTableDesc);
        HTable SVDTable = new HTable(configuration,Tools.SVD_TABLE_NAME);

        HTableDescriptor laplacianTableDesc = new HTableDescriptor(Tools.LAPLACIAN_TABLE_NAME);
        HColumnDescriptor laplacianColumnDescriptor = new HColumnDescriptor(Tools.LAPLACIAN_FAMILY_NAME) ;
        laplacianColumnDescriptor.setMaxVersions(1);
        laplacianTableDesc.addFamily(laplacianColumnDescriptor);
        laplacianTableDesc.setMaxFileSize( Tools.HTABLE);
        admin.createTable(laplacianTableDesc);

        HTableDescriptor qTableDesc = new HTableDescriptor(Tools.Q_TABLE_NAME);
        HColumnDescriptor qColumnDescriptor = new HColumnDescriptor(Tools.Q_FAMILY_NAME) ;
        qColumnDescriptor.setMaxVersions(1);
        qTableDesc.addFamily(qColumnDescriptor);
        admin.createTable(qTableDesc);

        HTableDescriptor rTableDesc = new HTableDescriptor(Tools.R_TABLE_NAME);
        HColumnDescriptor rColumnDescriptor = new HColumnDescriptor(Tools.R_FAMILY_NAME) ;
        rColumnDescriptor.setMaxVersions(1);
        rTableDesc.addFamily(rColumnDescriptor);
        admin.createTable(rTableDesc);

        HTableDescriptor kmeansTableDesc = new HTableDescriptor(Tools.KMEANS_TABLE_NAME);
        HColumnDescriptor kmeansColumnDescriptor = new HColumnDescriptor(Tools.KMEANS_FAMILY_NAME) ;
        kmeansColumnDescriptor.setMaxVersions(1);
        kmeansTableDesc.addFamily(kmeansColumnDescriptor);
        admin.createTable(kmeansTableDesc);

    }
    public static void main(String[] args) throws Exception {
        System.out.println(args[0]);
        if(args.length != 7) {
            System.out.println(args.length);
            return;
        }else{
            Tools.ROW = Long.valueOf(args[0]);
            Tools.COL = Long.valueOf(args[1]);
            Tools.K = Integer.valueOf(args[2]);
            Tools.X = Integer.valueOf(args[3]);
            Tools.OMG = Double.valueOf(args[4]);
            Tools.QR = Integer.valueOf(args[5]);
            Tools.KMEANS = Integer.valueOf(args[6]);
            Tools.HTABLE =  Integer.valueOf(args[7]);

        initHbase();
        Configuration conf = new Configuration();
        Path tempDir =  new Path( "Spectral");
        Path input = new Path("input");
        Path init = new Path(tempDir,"init");
        InitInputJob.runJob(input,init);
        DiagonalizeJob.runJob(init);
        LaplacianJob.runJob(init);
        QrJob.iter( Tools.QR);
        SortJob.runJob();
        KMeansJob.iter(Tools.KMEANS);
        printResult();
        }
    }

    public static void  printResult() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable svdTable = new HTable(configuration, Tools.SVD_TABLE_NAME);
        ResultScanner rs = null;

        Scan scan = new Scan();
        scan.addColumn(Tools.SVD_FAMILY_NAME.getBytes(),Tools.SVD_VALUE_NAME.getBytes());
        rs =  svdTable.getScanner(scan);
        HashMap<Integer,ArrayList<Integer>> hashMap = new HashMap<>();
        for(int i = 0 ; i < Tools.X ; i ++){
            hashMap.put(i,new ArrayList<Integer>());
        }
        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            if(cells != null ){
                for (Cell cell : cells) {
                    hashMap.get(Integer.valueOf(new String(CellUtil.cloneValue(cell)))).add(Integer.valueOf(new String(r.getRow())));
                }
            }
        }

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
}
