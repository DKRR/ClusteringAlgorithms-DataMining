package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
        //file 1 = current centroid file;
        //prev file = centroid iter-1;

    }


}
