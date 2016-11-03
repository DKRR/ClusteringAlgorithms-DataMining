package com.ub.cse601.project2.run;

/**
 * Created by nitish on 11/2/16.
 */

import com.sun.imageio.spi.FileImageInputStreamSpi;
import com.sun.xml.bind.v2.TODO;
import com.ub.cse601.project2.hadoop.KMeansMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Scanner;


public class RunHadoopMR {

    public static void main (String[] args) {

        try {

            Scanner sc = new Scanner(System.in);
            /*System.out.println("Enter File name of data set: ");
            String fileName = sc.next();

            if (fileName == null || fileName.length() == 0) {

                fileName = "cho.txt";

            }*/

            String path = "data/input/";
            //KMeansMR mapReduceObject = new KMeansMR(5, fileName, 100);
            KMeansMR mapReduceObject = new KMeansMR(5, "cho.txt", 100);
            mapReduceObject.readGeneDataSet(path);
            mapReduceObject.writeInitialCentroidsToFile(path, "initialCentroids.txt");

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setMapperClass(KMeansMR.KMeansMapper.class);
            job.setReducerClass(KMeansMR.KMeansReducer.class);
            job.setJarByClass(KMeansMR.class);

            //TODO: put while loop to check for convergence

            Path in = new Path("data/input/");
            Path out = new Path("data/output/");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);

            job.waitForCompletion(true);


        } catch ( Exception e ) {



        }

    }

}
