package edu.cqupt.spectral;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.cqupt.spectral.conf.Tools;
import edu.cqupt.spectral.diagonalize.DiagonalizeJob;
import edu.cqupt.spectral.input.InitInputJob;
import edu.cqupt.spectral.laplacian.LaplacianJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.mahout.common.HadoopUtil;

/**
 * Created with IntelliJ IDEA.
 * User: Ethan
 * Date: 1/15/16
 * Time: 10:19 AM
 * To change this template use File | Settings | File Templates.
 */
public class Spectral{


        // Construct the affinity matrix using the newly-created sequence files
//        DistributedRowMatrix A = new DistributedRowMatrix(affSeqFiles, new Path(outputTmp, "afftmp"), numDims, numDims);

//        Configuration depConf = new Configuration(conf);
//        A.setConf(depConf);

        // Construct the diagonal matrix D (represented as a vector)
//        Vector D = MatrixDiagonalizeJob.runJob(affSeqFiles, numDims);

        // Calculate the normalized Laplacian of the form: L = D^(-0.5)AD^(-0.5)
//        DistributedRowMatrix L = VectorMatrixMultiplicationJob.runJob(affSeqFiles, D, new Path(outputCalc, "laplacian"),
//                new Path(outputCalc, outputCalc));
//        L.setConf(depConf);
//
//        Path data;

        // SSVD requires an array of Paths to function. So we pass in an array of length one
//        Path[] LPath = new Path[1];
//        LPath[0] = L.getRowPath();
//
//        Path SSVDout = new Path(outputCalc, "SSVD");
//
//        SSVDSolver solveIt = new SSVDSolver(depConf, LPath, SSVDout, blockHeight, clusters, oversampling, numReducers);
//
//        solveIt.setComputeV(false);
//        solveIt.setComputeU(true);
//        solveIt.setOverwrite(true);
//        solveIt.setQ(poweriters);
//        solveIt.setBroadcast(false);
//        solveIt.run();
//        data = new Path(solveIt.getUPath());
//
        // Normalize the rows of Wt to unit length
        // normalize is important because it reduces the occurrence of two unique clusters combining into one
//        Path unitVectors = new Path(outputCalc, "unitvectors");
//
//        UnitVectorizerJob.runJob(data, unitVectors);
//
//        DistributedRowMatrix Wt = new DistributedRowMatrix(unitVectors, new Path(unitVectors, "tmp"), clusters, numDims);
//        Wt.setConf(depConf);
//        data = Wt.getRowPath();

        // Generate initial clusters using EigenSeedGenerator which picks rows as centroids if that row contains max
        // eigen value in that column
//        Path initialclusters = EigenSeedGenerator.buildFromEigens(conf, data,
//                new Path(output, Cluster.INITIAL_CLUSTERS_DIR), clusters, measure);
//
//        Run the KMeansDriver
//        Path answer = new Path(output, "kmeans_out");
//        KMeansDriver.run(conf, data, initialclusters, answer, convergenceDelta, maxIterations, true, 0.0, false);

//        Restore name to id mapping and read through the cluster assignments
//        Path mappingPath = new Path(new Path(conf.get("hadoop.tmp.dir")), "generic_input_mapping");
//        List<String> mapping = new ArrayList<>();
//        FileSystem fs = FileSystem.get(mappingPath.toUri(), conf);
//        if (fs.exists(mappingPath)) {
//            SequenceFile.Reader reader = new SequenceFile.Reader(fs, mappingPath, conf);
//            Text mappingValue = new Text();
//            IntWritable mappingIndex = new IntWritable();
//            while (reader.next(mappingIndex, mappingValue)) {
//                String s = mappingValue.toString();
//                mapping.add(s);
//            }
//            HadoopUtil.delete(conf, mappingPath);
//        } else {
//            log.warn("generic input mapping file not found!");
//        }
//
//        Path clusteredPointsPath = new Path(answer, "clusteredPoints");
//        Path inputPath = new Path(clusteredPointsPath, "part-m-00000");
//        int id = 0;
//        for (Pair<IntWritable, WeightedVectorWritable> record :
//                new SequenceFileIterable<IntWritable, WeightedVectorWritable>(inputPath, conf)) {
//            if (!mapping.isEmpty()) {
//                log.info("{}: {}", mapping.get(id++), record.getFirst().get());
//            } else {
//                log.info("{}: {}", id++, record.getFirst().get());
//            }

//public static class TokenizerMapper
//        extends Mapper<Object, Text, Text, IntWritable>{
//
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//
//    public void map(Object key, Text value, Context context
//    ) throws IOException, InterruptedException {
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        while (itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }
//    }
//}
//
//public static class IntSumReducer
//        extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//    public void reduce(Text key, Iterable<IntWritable> values,
//                       Context context
//    ) throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
//        result.set(sum);
//        context.write(key, result);
//    }
//}
    public static void initHbase() throws IOException {
        Long row = Tools.ROW;
        Long col = Tools.COL;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "scmhadoop-1");
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
        
        HTableDescriptor initTableDesc = new HTableDescriptor(Tools.INIT_TABLE_NAME);
        initTableDesc.addFamily(new HColumnDescriptor(Tools.INIT_FAMILY_NAME));
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
        affinityTableDesc.addFamily(new HColumnDescriptor(Tools.AFFINITY_FAMILY_NAME));
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
        admin.createTable(SVDTableDesc);
        HTable SVDTable = new HTable(configuration,Tools.SVD_TABLE_NAME);

        List<Put> SVDPuts = new ArrayList<Put>();
        for(Long i  = 0L ; i < row ; i++){
            Put put = new Put(i.toString().getBytes());
            put.setWriteToWAL(false);
            put.add(Tools.SVD_FAMILY_NAME.getBytes(),Tools.SVD_VALUE_NAME.toString().getBytes(),"1".getBytes());
            SVDPuts.add(put);
            if(SVDPuts.size() > 100){
                SVDTable.put(SVDPuts);
                SVDPuts.clear();
            }
        }
        SVDTable.put(SVDPuts);


        HTableDescriptor laplacianTableDesc = new HTableDescriptor(Tools.LAPLACIAN_TABLE_NAME);
        laplacianTableDesc.addFamily(new HColumnDescriptor(Tools.LAPLACIAN_FAMILY_NAME));
        admin.createTable(laplacianTableDesc);
//        Job job = new Job(configuration,"initHBase");

    }
    public static void main(String[] args) throws Exception {

        initHbase();
       int numDims =100;
        Configuration conf = new Configuration();
        Path tempDir =  new Path( "Spectral");
        HadoopUtil.delete(conf, tempDir);
        InitInputJob.runJob();
        DiagonalizeJob.runJob();
        LaplacianJob.runJob();


    }
}
