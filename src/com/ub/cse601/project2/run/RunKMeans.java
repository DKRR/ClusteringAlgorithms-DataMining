package com.ub.cse601.project2.run;

import com.ub.cse601.project2.clustering.KMeans;

import java.util.Scanner;

/**
 * Created by VenkataRamesh on 10/28/2016.
 */
public class RunKMeans {

    public static void main(String[] args) throws Exception{
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter No. of Clusters to start with: ");
        int k = sc.nextInt();
        System.out.println("Enter File name of data set: ");
        String fileName = 5sc.next();
        if(fileName==null || fileName.length()==0) fileName = "cho.txt";
        String path = "data/";
        System.out.println("Enter Max Iterations: ");
        Integer maxIter = sc.nextInt();
        KMeans kMeans = new KMeans(k,fileName,maxIter);
        kMeans.readGeneDataSet(path);
        double[][] distanceMatrix = kMeans.calculateDistanceMatrix();
        double[][] initKMeans = kMeans.initKMeans();
        kMeans.runKMeans(initKMeans);
    }
}
