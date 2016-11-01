package com.ub.cse601.project2.clustering;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Created by VenkataRamesh on 10/28/2016.
 */
public class KMeans {
    private Integer maxIterations;
    private Integer initialCentroids;
    private String fileName;
    private double[][] dataMatrix;
    private Map<Integer, List<double[][]>> clusters;
    private int objectCount;
    private int attributeCount;
    private int clusterIndex;


    public KMeans(Integer initialCentroids, String fileName, Integer maxIterations) {
        this.initialCentroids = initialCentroids;
        this.fileName = fileName;
        this.maxIterations = maxIterations;
    }


    public KMeans(String fileName) {
        this.fileName = fileName;
    }

    public double[][] readGeneDataSet(String path) throws IOException, FileNotFoundException {
        Path filePath = null;
        try {
            filePath = Paths.get(path, fileName);
            Stream<String> genes = Files.lines(filePath, StandardCharsets.UTF_8);
            List<String> geneData = Files.readAllLines(filePath, StandardCharsets.UTF_8);
            int rows = geneData.size();
            this.objectCount = rows;
            int columns = geneData.get(0).split("\t").length;
            clusterIndex = columns;
            this.attributeCount = columns - 2;
            dataMatrix = new double[rows][columns + 1];
            for (int i = 0; i < rows; i++) {
                String[] geneAttributes = geneData.get(i).split("\t");
                for (int j = 0; j < columns - 1; j++) {
                    if (j == 0) dataMatrix[i][j] = Double.parseDouble(geneAttributes[j]);
                    else dataMatrix[i][j] = Double.parseDouble(geneAttributes[j + 1]);
                }
                dataMatrix[i][columns - 1] = Double.parseDouble(geneAttributes[1]);
            }



        } catch (Exception ex) {

        }

        return dataMatrix;
    }

    public double[][] calculateDistanceMatrix() {
        double[][] distanceMatrix = new double[dataMatrix.length][dataMatrix.length];
        for (int i = 0; i < dataMatrix.length - 1; i++) {
            for (int j = i + 1; j < dataMatrix.length; j++) {
                double[] obj1 = dataMatrix[i];
                double[] obj2 = dataMatrix[j];
                double squaredSum = 0;
                for (int dim = 1; dim < obj1.length - 1; dim++) {
                    squaredSum += Math.pow(obj1[dim] - obj2[dim], 2);
                }
                double eucDistance = Math.sqrt(squaredSum);
                distanceMatrix[i][j] = eucDistance;
                distanceMatrix[j][i] = eucDistance;
            }
        }
        return distanceMatrix;
    }

    public double[][] initKMeans() {
        double[][] kMeans = new double[initialCentroids][];
        int k = 0;
        Random rand = new Random();
        List<Integer> clusterIndices = new ArrayList<Integer>();
        while (k < initialCentroids) {
            int centroidIndex = rand.nextInt(dataMatrix.length);
            if (clusterIndices.contains(centroidIndex)) continue;
            else {
                clusterIndices.add(centroidIndex);
                kMeans[k] = Arrays.copyOfRange(dataMatrix[centroidIndex], 1, attributeCount + 1);
                Arrays.stream(kMeans[k]).forEach(x -> {
                    x = new BigDecimal(x).setScale(2, RoundingMode.HALF_UP).doubleValue();
                });
                k++;
            }
        }
        return kMeans;
    }

    public void runKMeans(double[][] kMeans) {
        boolean converged = false;
        int iterations = 1;
        while (!converged && iterations <= maxIterations) {
            System.out.println("Running K-Means iteration:" + iterations);
            for (int i = 0; i < dataMatrix.length; i++) {
                double squaredDistance = 0;
                double eucdDis = 0;
                double minDistance = Double.MAX_VALUE;
                int minClusterIndex = 0;
                for (int k = 0; k < kMeans.length; k++) {
                    for (int j = 1; j <= attributeCount; j++) {
                        squaredDistance += Math.pow(dataMatrix[i][j] - kMeans[k][j - 1], 2);
                    }
                    eucdDis = Math.sqrt(squaredDistance);
                    if (eucdDis < minDistance) {
                        minDistance = eucdDis;
                        minClusterIndex = k + 1;
                    }
                    squaredDistance = 0;
                }
                dataMatrix[i][clusterIndex] = minClusterIndex;
            }
            double[][] newKMeans = new double[kMeans.length][attributeCount];
            for (int k = 0; k < kMeans.length; k++) {
                double objectCluster = k + 1;
                List<double[]> clusterObjects = Arrays.stream(dataMatrix).filter(x -> x[clusterIndex] == objectCluster).collect(Collectors.toList());
                for (int col = 1; col <= attributeCount; col++) {
                    double sum = 0;
                    for (double[] clusterObject : clusterObjects) {
                        sum += clusterObject[col];
                    }
                    double centroid = sum / clusterObjects.size();
                    newKMeans[k][col - 1] = new BigDecimal(centroid).setScale(2, RoundingMode.HALF_UP).doubleValue();
                }
            }
            if (checkConvergence(kMeans, newKMeans)) converged = true;
            iterations++;
            kMeans = newKMeans;
        }
        System.out.println("K-Means converged after " + (iterations - 1) + " iterations");
        printClusters(kMeans);
        System.out.println("Performing Cluster validation...");
        System.out.println("Jaccard Coefficient: " + calaculateJaccardCoefficient());
    }

    private boolean checkConvergence(double[][] oldKMeans, double[][] newKMeans) {
        return Arrays.deepEquals(oldKMeans, newKMeans);
    }

    private void printClusters(double[][] kMeans) {
        for (int i = 0; i < kMeans.length; i++) {
            double clsIndx = i + 1;
            List<double[]> clusterObjects = Arrays.stream(dataMatrix).filter(x -> x[clusterIndex] == clsIndx).collect(Collectors.toList());
            Arrays.stream(dataMatrix).filter(x -> x[clusterIndex] == clsIndx).collect(Collectors.toList());
            System.out.println("Cluster " + new Double(clsIndx).intValue() + " size: " + clusterObjects.size());
        }
    }

    private double calaculateJaccardCoefficient() {
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
