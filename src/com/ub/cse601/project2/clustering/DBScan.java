package com.ub.cse601.project2.clustering;

import org.apache.commons.math3.linear.RealMatrix;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by VenkataRamesh on 11/1/2016.
 */
public class DBScan {

    private String fileName;
    private double[][] dataMatrix;
    private double[][] distanceMatrix;
    private int objectCount;
    private int attributeCount;
    private int clusterIndex;
    private int minPoints;
    private double epsilon;
    private int visited;
    private boolean normalizePCA;


    public DBScan(String fileName) {
        this.fileName = fileName;

    }

    public DBScan(String fileName, Integer minPoints, double epsilon, boolean normalizePCA) {

        this.minPoints = minPoints;
        this.fileName = fileName;
        this.epsilon = epsilon;
        this.normalizePCA = normalizePCA;

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
            dataMatrix = new double[rows][columns + 2];
            for (int i = 0; i < rows; i++) {
                String[] geneAttributes = geneData.get(i).split("\t");
                for (int j = 0; j < columns - 1; j++) {
                    if (j == 0) dataMatrix[i][j] = Double.parseDouble(geneAttributes[j]);
                    else dataMatrix[i][j] = Double.parseDouble(geneAttributes[j + 1]);
                }
                dataMatrix[i][columns - 1] = Double.parseDouble(geneAttributes[1]);
                dataMatrix[i][clusterIndex] = 0;
                dataMatrix[i][columns + 1] = 0;
                this.visited = columns + 1;
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
        this.distanceMatrix = distanceMatrix;
        return distanceMatrix;
    }

    public double[][] runDBScan() throws Exception {
        int clusterNumber = 1;
        for (int i = 0; i < dataMatrix.length; i++) {
            if (dataMatrix[i][visited] == 0) {
                dataMatrix[i][visited] = 1;
                List<Integer> episilonIndex = regionQuery(i);
                if (episilonIndex.size() < minPoints) {
                    dataMatrix[i][visited] = Double.NEGATIVE_INFINITY;
                    dataMatrix[i][clusterIndex] = Double.NEGATIVE_INFINITY;
                } else {
                    expandCluster(i, episilonIndex, clusterNumber);
                    clusterNumber = clusterNumber + 1;
                }
            }
        }
        printClusters();
        System.out.println("Runnning Cluster Validation for DBScan, Jaccard Coeffcient: " + calaculateJaccardCoefficient());
        System.out.println("Running PCA Analysis and generating scatter plot...");
        createScatterPlot("DBScan");
        return dataMatrix;
    }

    public List<Integer> regionQuery(int index) {
        int count = 0;
        List<Integer> episilonIndex = new ArrayList<Integer>();
        for (int j = 0; j < distanceMatrix.length; j++) {
            if (distanceMatrix[index][j] <= epsilon && dataMatrix[index][clusterIndex] != Double.NEGATIVE_INFINITY) {
                episilonIndex.add(j);
            }
        }
        return episilonIndex;
    }


    public void expandCluster(int index, List<Integer> episilonIndex, int clusterNumber) {
        dataMatrix[index][clusterIndex] = clusterNumber;
        ListIterator<Integer> it = episilonIndex.listIterator();
        while (it.hasNext()) {
            int i = it.next();
            if (dataMatrix[i][visited] == 0) {
                dataMatrix[i][visited] = 1;
                List<Integer> neighbourEpisilonIndex = regionQuery(i);
                if (neighbourEpisilonIndex.size() >= minPoints) {
                    for (Integer neighBour : neighbourEpisilonIndex) {
                        it.add(neighBour);
                    }
                }
            }
            if (dataMatrix[i][clusterIndex] == 0) {
                dataMatrix[i][clusterIndex] = clusterNumber;
            }
        }
    }

    public void printClusters() {
        Map<Double, List<double[]>> clusterMap = new TreeMap<Double, List<double[]>>(Arrays.stream(dataMatrix).filter(x -> x[clusterIndex] != Double.NEGATIVE_INFINITY).
                collect(Collectors.groupingBy(x -> x[clusterIndex], Collectors.mapping(x -> x, Collectors.toList()))));
        clusterMap.forEach((x, y) -> {
            System.out.println("Cluster " + Double.valueOf(x).intValue() + " size: " + y.size());
        });
        System.out.println("Total clusters formed: " + clusterMap.size());
    }

    private double calaculateJaccardCoefficient() {
        int countAgree = 0;
        int countDisagree = 0;
        for (int i = 0; i < dataMatrix.length - 1; i++) {
            for (int j = i; j < dataMatrix.length; j++) {
                if (dataMatrix[i][clusterIndex] != Double.NEGATIVE_INFINITY && dataMatrix[i][clusterIndex] == dataMatrix[j][clusterIndex] &&
                        dataMatrix[i][clusterIndex - 1] == dataMatrix[j][clusterIndex - 1] &&
                        dataMatrix[i][clusterIndex - 1] != -1) {
                    countAgree++;
                } else if (dataMatrix[i][clusterIndex] != Double.NEGATIVE_INFINITY && dataMatrix[i][clusterIndex] == dataMatrix[j][clusterIndex] &&
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

    private void createScatterPlot(String title) {

        PCAAnalysis pca = new PCAAnalysis();
        RealMatrix featureMatrix = pca.prepareFeatureMatrix(dataMatrix, clusterIndex, clusterIndex - 2, normalizePCA);
        RealMatrix covMatrix = pca.covarianceMatrix(featureMatrix);
        RealMatrix principalComponents = pca.performEigenDecomposition(covMatrix, featureMatrix);
        double[] scaleX = pca.findXScale(principalComponents);
        double[] scaleY = pca.findYScale(principalComponents);
        for (int i = 0; i < principalComponents.getRowDimension(); i++) {
            dataMatrix[i][1] = principalComponents.getEntry(i, 0);
            dataMatrix[i][2] = principalComponents.getEntry(i, 1);
        }
        PCAScatterPlot.launchClass(dataMatrix, title, scaleX, scaleY, clusterIndex);

    }


}
