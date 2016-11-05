package com.ub.cse601.project2.run;


import com.ub.cse601.project2.clustering.HierarchialClustering;

import java.util.Scanner;

/**
 * Created by VenkataRamesh on 10/31/2016.
 */
public class RunHierarchialClustering {

    public static void main(String[] args) throws Exception {

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter File name of data set: ");
        String fileName = sc.next();
        System.out.println("Enter Final No. of Clusters: ");
        int k = sc.nextInt();
        if (fileName == null || fileName.length() == 0) fileName = "cho.txt";
        String path = "data/";
        boolean normalizePCA = false;
        System.out.println("Do you want to normalize dat for PCA, Please type 'Y' or 'N': ");
        String normalize = sc.next();
        if(normalize.equalsIgnoreCase("y")){
            normalizePCA = true;
        }
        HierarchialClustering hc = new HierarchialClustering(k, fileName, normalizePCA);
        hc.readGeneDataSet(path);
        double[][] distanceMatrix = hc.distanceMatrix();
        hc.runHierarchialMinClustering();


    }

}
