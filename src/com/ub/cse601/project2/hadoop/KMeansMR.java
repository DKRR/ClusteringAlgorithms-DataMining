package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//import javax.security.auth.login.Configuration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import javax.xml.soap.Text;
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by nitish on 10/31/16.
 */
public class KMeansMR {

    private int initialCentroids;
    private String fileName;
    private String initialCentroidFileName;
    private int objectCount;
    private int attributeCount;
    private int clusterIndex;
    private double[][] dataMatrix;
    private Integer maxIterations;
    private Map<Integer, List<double[][]>> clusters;


    public KMeansMR(Integer initialCentroids, String fileName, Integer maxIterations) {

        this.initialCentroids = initialCentroids;
        this.fileName = fileName;
        this.maxIterations = maxIterations;

    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        private ArrayList<Double[]> centroidList = new ArrayList<Double[]>();
        private ArrayList<Double[]> geneIndexExpList = new ArrayList<Double[]>();


        protected void setup(Context context) throws IOException, InterruptedException {

            try {


                super.setup(context);
                Configuration conf = context.getConfiguration();

                String filePath = "data/input/";
                String fileName = "initialCentroids.txt";
                Path centroidFilePath = Paths.get(filePath, fileName);

                List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);
                //TODO: check whether you need to remove last row

                for ( String singleGeneString : centroidData ) {

                    if ( singleGeneString != null || singleGeneString != "" ) {

                        String[] centroidExpressions = singleGeneString.split("\t");
                        Double[] singleCentroidExpressionStore = new Double[singleGeneString.split("\t").length];

                        for ( int i = 0; i < centroidExpressions.length; i++ ) {

                            singleCentroidExpressionStore[i] = Double.valueOf(centroidExpressions[i]);

                        }

                        centroidList.add(singleCentroidExpressionStore);
                        System.out.println("centroid values " +singleCentroidExpressionStore.toString());

                    }

                }

                System.out.println("centorid list = " + centroidList.toString());

                /*String dataFilePath = "data/";
                String dataFileName = "cho.txt";
                Path genePath = Paths.get(dataFilePath, dataFileName);

                List<String> geneIndexExpData = Files.readAllLines(genePath, StandardCharsets.UTF_8);

                for(String eachGeneString : geneIndexExpData){

                    String[] eachGeneExpValues = eachGeneString.split("\t");
                    Double[] singleGeneExpressionStore = new Double[eachGeneString.split("\t").length];

                    for ( int i = 0; i < eachGeneExpValues.length; i++ ) {

                        if(i>1) {
                            singleGeneExpressionStore[i-1] = Double.valueOf(eachGeneExpValues[i]);
                        }
                        else{
                            singleGeneExpressionStore[i] = Double.valueOf(eachGeneExpValues[i]);
                        }

                    }

                    geneIndexExpList.add(singleGeneExpressionStore);
                }*/


            } catch ( Exception e ) {

                e.printStackTrace();

            }

        }


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String geneLine = value.toString();
            //System.out.println("mapper value =" + geneLine);
            String[] eachGeneExpValues = geneLine.split("\t");
            Double[] singleExpValue = new Double[geneLine.split("\t").length - 1];

            //System.out.println("mapper single expression length = " + singleExpValue.length);

            for ( int i = 0; i < eachGeneExpValues.length; i++ ) {

                if(i>1) {
                    singleExpValue[i-1] = Double.valueOf(eachGeneExpValues[i]);
                }
                else{
                    singleExpValue[i] = Double.valueOf(eachGeneExpValues[i]);
                }

            }

            //Double[] singleExpValue = geneIndexExpList.get(0);
            int centroidIndex = 0;
            int c = 0;
            double closestCentroidDist = Double.MAX_VALUE;


            for (Double[] eachCentroid : centroidList){

                double squaredSum = 0;
                for (int i = 1; i<singleExpValue.length; i++) {
                    squaredSum += Math.pow(singleExpValue[i] - eachCentroid[i-1], 2);
                }
                double eucDistance = Math.sqrt(squaredSum);
                if(eucDistance < closestCentroidDist){
                    closestCentroidDist = eucDistance;
                    //System.out.println("euclid dist =" + closestCentroidDist);
                    centroidIndex = c;
                }
                c = c + 1;
            }

            Double[] closestCentroid = centroidList.get(centroidIndex);

            Text mapKeyOutput = new Text();
            Text mapValueOutput = new Text();

            mapKeyOutput.set(closestCentroid.toString());
            mapValueOutput.set(singleExpValue.toString());

            context.write(mapKeyOutput, value);


        }

    }


    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        private ArrayList<Double[]> centroidList = new ArrayList<Double[]>();
        private ArrayList<Double[]> geneIndexExpList = new ArrayList<Double[]>();
        private int centroidLength = 0;


        protected void setup(Context context) throws IOException, InterruptedException {

            try {


                super.setup(context);
                Configuration conf = context.getConfiguration();

                String filePath = "data/input/";
                String fileName = "initialCentroids.txt";
                Path centroidFilePath = Paths.get(filePath, fileName);

                List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);
                //TODO: check whether you need to remove last row

                for ( String singleGeneString : centroidData ) {

                    if ( singleGeneString != null || singleGeneString != "" ) {

                        String[] centroidExpressions = singleGeneString.split("\t");
                        centroidLength = centroidExpressions.length;
                        //System.out.println("length =" + centroidExpressions.length);
                        Double[] singleCentroidExpressionStore = new Double[singleGeneString.split("\t").length];

                        for ( int i = 0; i < centroidExpressions.length; i++ ) {

                            singleCentroidExpressionStore[i] = Double.valueOf(centroidExpressions[i]);

                        }

                        centroidList.add(singleCentroidExpressionStore);

                    }

                }
                //Double[] newCentroidExp = new Double[centroidLength];

            } catch ( Exception e ) {

                e.printStackTrace();

            }

        }


        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            /*String[] tempLength = value.toString().split("\t");
            String tempStr = value.toString();
            System.out.println("value is "+value);
            int tlen = tempLength.length;
            System.out.println("len is "+tlen);*/

            //int genesLength = (value.toString().split("\t").length) - 2;
            //System.out.println(genesLength);
            //String reduceKeyString = key.toString();
            //System.out.println("key value string =" + reduceKeyString);
            //Double[] newCentroidExp = new Double[reduceKeyString.split("\t").length];
            Double[] newCentroidExp = new Double[centroidLength];
            Arrays.fill(newCentroidExp, 0.00);

            int genesLength = centroidLength;
            System.out.println(genesLength);

            int genesCount = 0;
            List<Integer> clusterGenePoints = new ArrayList<Integer>();

            for (Text eachValue: value) {


                genesCount = genesCount + 1;
                String singleGeneExpression = eachValue.toString();
                String[] singleGeneExpSplit = singleGeneExpression.split("\t");
                //System.out.println(Arrays.toString(singleGeneExpSplit));

                //System.out.println("singleGeneExpSplit[0] = " + singleGeneExpSplit[0]);
                clusterGenePoints.add(Integer.parseInt(singleGeneExpSplit[0]));
                Double[] singleGeneDouble = new Double[singleGeneExpression.split("\t").length];

                //System.out.println("singlgenelength =" + singleGeneExpSplit.length);

                for ( int i = 0; i < singleGeneExpSplit.length; i++) {

                    singleGeneDouble[i] = Double.valueOf(singleGeneExpSplit[i]);
                    //System.out.println("data exps = " + singleGeneDouble[i]);

                    if(i>1){
                        //System.out.println("newcentroid value = " + newCentroidExp[i-2]);
                        newCentroidExp[i-2] = newCentroidExp[i-2] + Double.valueOf(singleGeneExpSplit[i]);
                    }


                }

            }

            for(int i = 0; i < genesLength; i ++){

                newCentroidExp[i] = (double)newCentroidExp[i]/genesCount;
            }

            String filePath = "data/";
            String fileName = "initialCentroids.txt";
            Path centroidFilePath = Paths.get(filePath, fileName);

            //Files.createFile(centroidFilePath);
            String strLine = Arrays.stream(newCentroidExp).map(Object::toString).collect(Collectors.joining("\t"));
            Files.write(centroidFilePath, strLine.getBytes(), StandardOpenOption.CREATE_NEW);

            context.write(new Text(Arrays.toString(newCentroidExp)), new Text(clusterGenePoints.stream().map(Object::toString).collect(Collectors.joining(","))));

        }

    }


    /*
    * This function picks a set of random initial centroids
     * from the original data-set and writes them to a new file.*/
    public void writeInitialCentroidsToFile(String filePath, String centroidFileName) throws Exception {

        try {

            double[][] kMeans = new double[initialCentroids][];

            int k = 0;

            Random rand = new Random();
            List<Integer> clusterIndices = new ArrayList<Integer>();
            Path file = Paths.get(filePath, centroidFileName);
            BufferedWriter writer = new BufferedWriter(new FileWriter(file.toFile()));

            //ch double count = 1;

            while (k < initialCentroids) {

                int centroidIndex = rand.nextInt(dataMatrix.length);

                if (clusterIndices.contains(centroidIndex)) {

                    continue;

                } else {

                    clusterIndices.add(centroidIndex);

                    kMeans[k] = Arrays.copyOfRange(dataMatrix[centroidIndex], 1, attributeCount + 1);

                    //kMeans[k][0] = count++;

                    Arrays.stream(kMeans[k]).forEach(x -> {
                        x = new BigDecimal(x).setScale(2, RoundingMode.HALF_UP).doubleValue();
                    });

                    k++;

                }

            }


            try {

                Arrays.stream(kMeans).forEach(singleArray -> {

                    try {

                        String line = Arrays.stream(singleArray).mapToObj(i -> String.valueOf(i)).collect(Collectors.joining("\t"));
                        System.out.println(line);

                        writer.append(line + "\n");
                        writer.flush();

                    } catch ( Exception ex ) {

                        ex.printStackTrace();

                    }


                });

            } catch ( Exception e ) {

                e.printStackTrace();

            } finally {

                writer.close();

            }


        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public void readGeneDataSet(String path) throws IOException {

        try {

            Path filePath = Paths.get(path, fileName);

            List<String> geneData = Files.readAllLines(filePath, StandardCharsets.UTF_8);

            int rows = geneData.size();
            this.objectCount = rows;
            int columns = geneData.get(0).split("\t").length;
            clusterIndex = columns;
            this.attributeCount = columns - 2;
            dataMatrix = new double[rows][columns + 1];

            for (int i = 0; i < rows; i++) {

                String[] geneAttributes = geneData.get(i).split("\t");

                for (int j = 0; j < columns - 1; j++) {

                    if (j == 0) {

                        dataMatrix[i][j] = Double.parseDouble(geneAttributes[j]);

                    } else {

                        dataMatrix[i][j] = Double.parseDouble(geneAttributes[j + 1]);

                    }

                }

                dataMatrix[i][columns - 1] = Double.parseDouble(geneAttributes[1]);

            }

        } catch (Exception e) {

            e.printStackTrace();

        }
    }


    public static void main(String[] args) {

        try {

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter File name of data set: ");
            String fileName = sc.next();

            if (fileName == null || fileName.length() == 0) {

                fileName = "cho.txt";

            }

            String path = "data/";
            KMeansMR mapReduceObject = new KMeansMR(5, fileName, 100);
            mapReduceObject.readGeneDataSet(path);
            mapReduceObject.writeInitialCentroidsToFile(path, "initialCentroids.txt");

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

}
