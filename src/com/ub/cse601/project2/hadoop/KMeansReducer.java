package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.stream.Collectors;


public class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {


    private int centroidLength = 0;
    private static final String ITERATION = "ITERATION";
    private Integer iter = 0;

    protected void setup(Context context) throws IOException, InterruptedException {


        try {

            super.setup(context);
            Configuration conf = context.getConfiguration();
            iter = conf.getInt(ITERATION, 0);

            String filePath = "data/centroids/";
            String fileName = "centroids_0.txt";
            Path centroidFilePath = Paths.get(filePath, fileName);

            List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);

            String singleGeneString = centroidData.get(0);
            String[] centroidExpressions = singleGeneString.split("\t");
            centroidLength = centroidExpressions.length;


        } catch (Exception e) {

            e.printStackTrace();

        }

    }


    protected void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {


        Double[] newCentroidExp = new Double[centroidLength];
        Arrays.fill(newCentroidExp, 0.00);

        int genesLength = centroidLength;

        int genesCount = 0;
        List<Integer> clusterGenePoints = new ArrayList<Integer>();

        for (Text eachValue : value) {


            genesCount = genesCount + 1;
            String singleGeneExpression = eachValue.toString();
            String[] singleGeneExpSplit = singleGeneExpression.split("\t");

            clusterGenePoints.add(Integer.parseInt(singleGeneExpSplit[0]));
            Double[] singleGeneDouble = new Double[singleGeneExpSplit.length];

            for (int i = 0; i < singleGeneExpSplit.length; i++) {

                singleGeneDouble[i] = Double.valueOf(singleGeneExpSplit[i]);

                if (i > 1) {
                    newCentroidExp[i - 2] = newCentroidExp[i - 2] + Double.valueOf(singleGeneExpSplit[i]);
                }


            }
        }

        for (int i = 0; i < genesLength; i++) {

            newCentroidExp[i] = newCentroidExp[i] / genesCount;
            newCentroidExp[i] = BigDecimal.valueOf(newCentroidExp[i]).setScale(2, RoundingMode.HALF_UP).doubleValue();
        }


        String filePath = "data/centroids/";
        String fileName = "centroids_" + String.valueOf(iter) + ".txt";
        Path centroidFilePath = Paths.get(filePath, fileName);

        String strLine = Arrays.stream(newCentroidExp).map(Object::toString).collect(Collectors.joining("\t"));
        if (Files.exists(centroidFilePath)) {
            String nextLine = "\n";
            Files.write(centroidFilePath, nextLine.getBytes(), StandardOpenOption.APPEND);
            Files.write(centroidFilePath, strLine.getBytes(), StandardOpenOption.APPEND);
        } else {
            Files.write(centroidFilePath, strLine.getBytes(), StandardOpenOption.CREATE);
        }

        context.write(new Text(key.toString()), new Text(clusterGenePoints.stream().map(Object::toString).collect(Collectors.joining(","))));

    }

}