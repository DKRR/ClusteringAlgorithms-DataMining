package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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

    public MRStarter(String inPath, String outPath, Integer maxIterations) {

        this.inPath = inPath;
        this.outPath = outPath;
        this.maxIterations = maxIterations;

    }

    public void runKMeansMR() throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        boolean converged = false;
        Integer iterations = 1;
        while (!converged && iterations <= maxIterations) {
            //delete output folder
            deleteOutPutFolder(outPath, conf);
            job.setJobName("Running KMeans MR, iteration: "+iterations);
            job.getConfiguration().setInt(ITERATION, iterations);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(MRStarter.class);
            Path in = new Path(inPath);
            Path out = new Path(outPath);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);
            job.waitForCompletion(true);
            converged = checkConvergence(iterations);
            iterations++;

        }
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

            String currentFileName = "Centroids_" + String.valueOf(iter);
            String previousFileName = "Centroids_" + String.valueOf(iter - 1);

            java.nio.file.Path currentCentroidFilePath = Paths.get(inPath, currentFileName);
            java.nio.file.Path previousCentroidFilePath = Paths.get(inPath, previousFileName);

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


}
