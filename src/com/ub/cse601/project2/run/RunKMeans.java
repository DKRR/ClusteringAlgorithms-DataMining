package com.ub.cse601.project2.run;

import com.ub.cse601.project2.clustering.KMeans;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by VenkataRamesh on 10/28/2016.
 */
public class RunKMeans {

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter File name of data set: ");
        String fileName = sc.next();
        if (fileName == null || fileName.length() == 0) fileName = "cho.txt";
        System.out.println("Enter No. of Clusters: ");
        int k = sc.nextInt();
        String path = "data/";
        System.out.println("Enter Max Iterations: ");
        Integer maxIter = sc.nextInt();
        KMeans kMeans = new KMeans(k, fileName, maxIter);
        kMeans.readGeneDataSet(path);
        double[][] distanceMatrix = kMeans.calculateDistanceMatrix();
        double[][] initKMeans = kMeans.initKMeans();
        /*double[][] tk = new double[5][];
        tk[0] = new double[]{-0.69, -0.96, -1.16, -0.66, -0.55, 0.12, -1.07, -1.22, 0.82, 1.4, 0.71, 0.68, 0.11, -0.04, 0.19, 0.82};
        tk[1] = new double[]{-0.2, 0.14, 0.73, 0.3, -0.28, -0.12, -0.27, -0.22, -0.25, 0.24, 0.07, 0.0, -0.1, -0.06, -0.17, -0.08};
        tk[2] = new double[]{-0.32, -0.21, 1.11, 0.84, -0.14, -0.09, -0.37, -0.59, -0.4, 0.44, 0.25, 0.04, 0.08, -0.304, -0.39, -0.36};
        tk[3] = new double[]{-0.48, 0.06, -0.01, 0.31, 0.37, 0.27, 0.35, 0.31, -0.19, -0.27, -0.23, 0.15, 0.024, 0.18, -0.24, -0.41};
        tk[4] = new double[]{-0.79, -0.56, -0.79, -0.23, -0.53, -0.14, 0.61, 0.95, 0.96, 0.38, -0.11, -0.31, -0.41, 0.49, 0.08, 0.15};*/
        System.out.println("Randomly chosen initial centroids:");
        Arrays.stream(initKMeans).forEach(x -> {
            System.out.println(Arrays.toString(x));
        });
        kMeans.runKMeans(initKMeans);
    }
}
