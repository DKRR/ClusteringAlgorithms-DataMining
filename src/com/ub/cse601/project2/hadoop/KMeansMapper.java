package com.ub.cse601.project2.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ArrayList<Double[]> centroidList = new ArrayList<Double[]>();
    private ArrayList<Double[]> geneIndexExpList = new ArrayList<Double[]>();
    private static final String ITERATION = "ITERATION";


    protected void setup(Context context) throws IOException, InterruptedException {

        try {


            super.setup(context);
            Configuration conf = context.getConfiguration();
            Integer iter = conf.getInt(ITERATION, 0)-1;
            String filePath = "data/input/";
            String fileName = "Centroids_"+iter;
            Path centroidFilePath = Paths.get(filePath, fileName);
            List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);
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