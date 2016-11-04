package com.ub.cse601.project2.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Created by ramesh on 11/3/16.
 */
public class MRStarter {

    private String inPath;
    private String outPath;
    private Integer maxIterations;
    private static final String ITERATION = "ITERATION";
    private double[][] dataMatrix;
    private int clusterIndex;

    public MRStarter(String inPath, String outPath, Integer maxIterations, double[][] dataMatrix, int clusterIndex ) {

        this.inPath = inPath;
        this.outPath = outPath;
        this.maxIterations = maxIterations;
        this.dataMatrix = dataMatrix;
        this.clusterIndex = clusterIndex;

    }

    public void runKMeansMR() throws Exception {


        Configuration conf = new Configuration();
        boolean converged = false;
        Integer iterations = 1;
        while (!converged && iterations <= maxIterations) {
            Job job = Job.getInstance(conf);
            //delete output folder
            deleteOutPutFolder(outPath, conf);
            job.setJobName("Running KMeans MR, iteration: "+iterations);
            job.getConfiguration().setInt(ITERATION, iterations);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(MRStarter.class);
            Path in = new Path(inPath);
            Path out = new Path(outPath);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);
            job.waitForCompletion(true);

            //job.waitForCompletion(true) ? 0 : 1;

            converged = checkConvergence(iterations);
            iterations++;

        }

        System.out.println("Iterations: " + iterations);
        System.out.println("Cluster Validation Jaccard Coefficient: " + calaculateJaccardCoefficient());

    }


    private void deleteOutPutFolder(String outPath, Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outPath);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

    }

    boolean checkConvergence(int iter){

        try {

            String currentFileName = "centroids_" + String.valueOf(iter) + ".txt";
            String previousFileName = "centroids_" + String.valueOf(iter - 1) + ".txt";

            String centroidPath = "data/centroids/";

            java.nio.file.Path currentCentroidFilePath = Paths.get(centroidPath, currentFileName);
            java.nio.file.Path previousCentroidFilePath = Paths.get(centroidPath, previousFileName);

            List<String> currentCentroids = Files.readAllLines(currentCentroidFilePath, StandardCharsets.UTF_8);
            List<String> previousCentroids = Files.readAllLines(previousCentroidFilePath, StandardCharsets.UTF_8);

            Collections.sort(currentCentroids);
            Collections.sort(previousCentroids);

            return currentCentroids.equals(previousCentroids);



        } catch ( Exception e ) {

            e.printStackTrace();

        }

        return false;

    }

    private double calaculateJaccardCoefficient() throws IOException {

        String filePath = "data/output/";
        String fileName = "part-r-00000";
        java.nio.file.Path centroidFilePath = Paths.get(filePath, fileName);

        List<String> finalData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);

        for ( String singleLine : finalData ) {

            String[] lineSplit = singleLine.split("\t");
            Double centroidIndex = Double.parseDouble(lineSplit[0]);

            Arrays.stream(lineSplit[1].split(",")).forEach(x -> {

                    int index = Integer.parseInt(x);
                    dataMatrix[index-1][clusterIndex] = centroidIndex;
            });

        }

        int countAgree = 0;
        int countDisagree = 0;

        for (int i = 0; i < dataMatrix.length - 1; i++) {
            for (int j = i; j < dataMatrix.length; j++) {
                if (dataMatrix[i][clusterIndex] == dataMatrix[j][clusterIndex] &&
                        dataMatrix[i][clusterIndex - 1] == dataMatrix[j][clusterIndex - 1] &&
                        dataMatrix[i][clusterIndex - 1] != -1) {
                    countAgree++;
                } else if (dataMatrix[i][clusterIndex] == dataMatrix[j][clusterIndex] &&
                        dataMatrix[i][clusterIndex - 1] != dataMatrix[j][clusterIndex - 1] ||
                        (dataMatrix[i][clusterIndex] != dataMatrix[j][clusterIndex] &&
                                dataMatrix[i][clusterIndex - 1] == dataMatrix[j][clusterIndex - 1]) &&
                                dataMatrix[i][clusterIndex - 1] != -1) {
                    countDisagree++;
                }

            }
        }
        return (double) countAgree / (countAgree + countDisagree);
    }


}
