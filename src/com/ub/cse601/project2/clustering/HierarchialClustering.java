package com.ub.cse601.project2.clustering;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttributeView;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

/**
 * Created by VenkataRamesh on 10/29/2016.
 */
public class
HierarchialClustering {

    private String fileName;
    private double[][] dataMatrix;
    private double[][] distanceMatrix;
    private int objectCount;
    private int attributeCount;
    private int noOfClusters;
    private int clusterIndex;
    private int maxIterations;

    public HierarchialClustering(String fileName) {
        this.fileName = fileName;

    }

    public HierarchialClustering(Integer noOfClusters, String fileName, Integer maxIterations) {
        this.noOfClusters = noOfClusters;
        this.fileName = fileName;
        this.maxIterations = maxIterations;
    }

    public double[][] readGeneDataSet(String path) throws Exception {
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
                dataMatrix[i][clusterIndex] = i + 1;
            }
        } catch (Exception ex) {

        }

        return dataMatrix;
    }

    public double[][] distanceMatrix() {
        double[][] distanceMatrix = new double[dataMatrix.length][dataMatrix.length];
        for (int i = 0; i < dataMatrix.length - 1; i++) {
            int colCount = dataMatrix[i].length;
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
            distanceMatrix[i][colCount - 1] = i + 1;
        }
        this.distanceMatrix = distanceMatrix;
        return distanceMatrix;

    }

    public void runHierarchialMinClustering() {
        int clusterCount = dataMatrix.length;
        int count = 1;
        while (clusterCount > noOfClusters && count <= maxIterations) {
            System.out.println("Running Hierarchial Clustering iteration: " + count++);
            int obj1Idx = 0;
            int obj2Idx = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < distanceMatrix.length - 1; i++) {
                for (int j = i + 1; j < distanceMatrix.length; j++) {
                    if (distanceMatrix[i][j] < minDistance && dataMatrix[i][clusterIndex] != dataMatrix[j][clusterIndex]) {
                        minDistance = distanceMatrix[i][j];
                        //get index of 2 min clusters
                        obj1Idx = i;
                        obj2Idx = j;
                    }
                }
            }

            //get actual cluster id for data points/clusters
            double grpi = dataMatrix[obj1Idx][clusterIndex];
            double grpj = dataMatrix[obj2Idx][clusterIndex];

            int[] grpiIndices = IntStream.range(0, dataMatrix.length)
                    .filter(i -> dataMatrix[i][clusterIndex] == grpi)
                    .toArray();
            int[] grpjIndices = IntStream.range(0, dataMatrix.length)
                    .filter(i -> dataMatrix[i][clusterIndex] == grpj)
                    .toArray();
            if (grpiIndices.length <= grpjIndices.length) {
                for (int index : grpiIndices) {
                    dataMatrix[index][clusterIndex] = dataMatrix[obj2Idx][clusterIndex];
                }
            } else {
                for (int index : grpjIndices) {
                    dataMatrix[index][clusterIndex] = dataMatrix[obj1Idx][clusterIndex];
                }
            }
            clusterCount = calculateClusterCount(dataMatrix);

        }
        System.out.println("Hierarchial Clustering converged after " + String.valueOf(count - 1) + " iterations");
        printClusters();
        System.out.println("Jaccard Coefficient: " + calaculateJaccardCoefficient());
    }


    public int calculateClusterCount(double[][] dataMatrix) {
        Map<Double, Long> clusterMap = Arrays.stream(dataMatrix).collect(Collectors.groupingBy(x -> x[clusterIndex], Collectors.counting()));
        return clusterMap.size();
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

    public void printClusters() {
        Map<Double, List<double[]>> clusterMap = new TreeMap<Double, List<double[]>>(Arrays.stream(dataMatrix).
                collect(Collectors.groupingBy(x -> x[clusterIndex], Collectors.mapping(x -> x, Collectors.toList()))));
        clusterMap.forEach((x, y) -> {
            System.out.println("Cluster " + Double.valueOf(x).intValue() + " size: " + y.size());
        });
        System.out.println("Total clusters formed: " + clusterMap.size());
    }
}



