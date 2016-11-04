package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private ArrayList<Double[]> centroidList = new ArrayList<Double[]>();
    private static final String ITERATION = "ITERATION";


    protected void setup(Context context) throws IOException, InterruptedException {

        try {


            super.setup(context);
            Configuration conf = context.getConfiguration();
            Integer iter = conf.getInt(ITERATION, 0) - 1;
            String filePath = "data/centroids/";
            String fileName = "centroids_" + iter + ".txt";
            Path centroidFilePath = Paths.get(filePath, fileName);
            List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);

            for (String singleGeneString : centroidData) {

                if (singleGeneString != null || singleGeneString != "") {

                    String[] centroidExpressions = singleGeneString.split("\t");
                    Double[] singleCentroidExpressionStore = new Double[singleGeneString.split("\t").length];

                    for (int i = 0; i < centroidExpressions.length; i++) {

                        singleCentroidExpressionStore[i] = Double.valueOf(centroidExpressions[i]);

                    }

                    centroidList.add(singleCentroidExpressionStore);
                }

            }


        } catch (Exception e) {

            e.printStackTrace();

        }

    }


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String geneLine = value.toString();

        String[] eachGeneExpValues = geneLine.split("\t");
        Double[] singleExpValue = new Double[geneLine.split("\t").length - 1];

        for (int i = 0; i < eachGeneExpValues.length; i++) {

            if (i > 1) {
                singleExpValue[i - 1] = Double.valueOf(eachGeneExpValues[i]);
            } else {
                singleExpValue[i] = Double.valueOf(eachGeneExpValues[i]);
            }

        }

        int centroidIndex = 0;
        int c = 0;
        double closestCentroidDist = Double.MAX_VALUE;


        for (Double[] eachCentroid : centroidList) {

            double squaredSum = 0;
            for (int i = 1; i < singleExpValue.length; i++) {
                squaredSum += Math.pow(singleExpValue[i] - eachCentroid[i - 1], 2);
            }
            double eucDistance = Math.sqrt(squaredSum);
            if (eucDistance < closestCentroidDist) {
                closestCentroidDist = eucDistance;
                centroidIndex = c;
            }
            c = c + 1;
        }

        int reduceKey = centroidIndex + 1;

        context.write(new IntWritable(reduceKey), value);


    }

}