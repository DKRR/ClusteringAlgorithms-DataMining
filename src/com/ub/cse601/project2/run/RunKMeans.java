package com.ub.cse601.project2.run;

import com.ub.cse601.project2.clustering.KMeans;
import javafx.application.Application;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.util.*;
import java.util.stream.Collectors;

import javafx.scene.Scene;

/**
 * Created by VenkataRamesh on 10/28/2016.
 */
public class RunKMeans {

    static double[][] dataMatrix = null;

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter File name of data set: ");
        String fileName = sc.nextLine();
        if (fileName == null || fileName.length() == 0) fileName = "cho.txt";
        String path = "data/";
        System.out.println("Do you want to input initial centroids, Please type 'Y' or 'N': ");
        String manual = sc.nextLine();
        List<Integer> geneIds = new ArrayList<Integer>();
        int k=0;
        if(manual.equalsIgnoreCase("y")){
            System.out.println("Enter comma separated gene id's: ");
            String genes = sc.nextLine().replaceAll("\\s+", "");
            System.out.println(genes);
            geneIds = Arrays.stream(genes.split(",")).collect(Collectors.mapping(x -> Integer.parseInt(x.trim()), Collectors.toList()));
            System.out.println(geneIds);
        }
        else{
            System.out.println("Enter No. of Clusters: ");
            k = sc.nextInt();
        }
        System.out.println("Enter Max Iterations: ");
        Integer maxIter = sc.nextInt();
        boolean normalizePCA = false;
        System.out.println("Do you want to normalize dat for PCA, Please type 'Y' or 'N': ");
        String normalize = sc.next();
        if(normalize.equalsIgnoreCase("y")){
            normalizePCA = true;
        }
        KMeans kMeans = new KMeans(k, fileName, maxIter, geneIds, normalizePCA);
        kMeans.readGeneDataSet(path);
        double[][] distanceMatrix = kMeans.calculateDistanceMatrix();
        double[][] initKMeans = kMeans.initKMeans();
        dataMatrix = kMeans.dataMatrix;
        /*double[][] tk = new double[5][];
        tk[0] = new double[]{-0.69, -0.96, -1.16, -0.66, -0.55, 0.12, -1.07, -1.22, 0.82, 1.4, 0.71, 0.68, 0.11, -0.04, 0.19, 0.82};
        tk[1] = new double[]{-0.2, 0.14, 0.73, 0.3, -0.28, -0.12, -0.27, -0.22, -0.25, 0.24, 0.07, 0.0, -0.1, -0.06, -0.17, -0.08};
        tk[2] = new double[]{-0.32, -0.21, 1.11, 0.84, -0.14, -0.09, -0.37, -0.59, -0.4, 0.44, 0.25, 0.04, 0.08, -0.304, -0.39, -0.36};
        tk[3] = new double[]{-0.48, 0.06, -0.01, 0.31, 0.37, 0.27, 0.35, 0.31, -0.19, -0.27, -0.23, 0.15, 0.024, 0.18, -0.24, -0.41};
        tk[4] = new double[]{-0.79, -0.56, -0.79, -0.23, -0.53, -0.14, 0.61, 0.95, 0.96, 0.38, -0.11, -0.31, -0.41, 0.49, 0.08, 0.15};*/
        if(manual.equalsIgnoreCase("Y")){
            System.out.println("Initial chosen centroids are:");
        }
        else{
            System.out.println("Randomly chosen initial centroids:");
        }
        Arrays.stream(initKMeans).forEach(x -> {
            System.out.println(Arrays.toString(x));
        });
        kMeans.runKMeans(initKMeans);

    }
}
