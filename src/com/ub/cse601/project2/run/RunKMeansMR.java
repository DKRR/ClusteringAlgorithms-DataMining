package com.ub.cse601.project2.run;

/**
 * Created by nitish on 11/2/16.
 */

import com.ub.cse601.project2.hadoop.MRStarter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class RunKMeansMR {

    private int initialCentroids;
    private String fileName;
    private String initialCentroidFileName;
    private int objectCount;
    private int attributeCount;
    private int clusterIndex;
    private double[][] dataMatrix;
    private Integer maxIterations;
    private Map<Integer, List<double[][]>> clusters;
    private String inPath;
    private String outPath;


    public RunKMeansMR(Integer initialCentroids, String fileName, Integer maxIterations) {
        this.initialCentroids = initialCentroids;
        this.fileName = fileName;
        this.maxIterations = maxIterations;
    }


    public static void main(String[] args) {

        try {

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter File name of data set: ");
            String fileName = sc.next();

            if (fileName == null || fileName.length() == 0) {

                fileName = "cho.txt";

            }

            System.out.println("Enter No. of Clusters: ");
            int k = sc.nextInt();

            System.out.println("Enter Max Iterations: ");
            Integer maxIter = sc.nextInt();

            //System.out.println("args" + args.length);

            //String inPath = !args.equals("") && args[0] != null || args[0].length() > 0 ? args[0] : "data/input/";
            //String outPath = !args.equals("") && args[1] != null || args[1].length() > 0 ? args[1] : "data/output/";

            String inPath = "data/input/";
            String outPath = "data/output/";


            RunKMeansMR kMeansMR = new RunKMeansMR(k, fileName, maxIter);
            kMeansMR.readGeneDataSet(inPath);
            double[][] initialKMeans = kMeansMR.initKMeans();
            kMeansMR.writeInitialCentroidsToFile(inPath, "Centroids_0.txt", initialKMeans);
            MRStarter startJob = new MRStarter(inPath, outPath, maxIter);
            startJob.runKMeansMR();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    private boolean checkConvergence(double[][] oldKMeans, double[][] newKMeans) {
        return Arrays.deepEquals(oldKMeans, newKMeans);
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


    public void readGeneDataSet(String path) throws IOException {

        try {

            System.out.println("I'm here");
            java.nio.file.Path filePath = Paths.get(path, fileName);

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

                    if (j == 0) {

                        dataMatrix[i][j] = Double.parseDouble(geneAttributes[j]);

                    } else {

                        dataMatrix[i][j] = Double.parseDouble(geneAttributes[j + 1]);

                    }

                }

                dataMatrix[i][columns - 1] = Double.parseDouble(geneAttributes[1]);

            }

        } catch (Exception e) {

            e.printStackTrace();

        }
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

    public void writeInitialCentroidsToFile(String filePath, String centroidFileName, double[][] initialCentroids) throws Exception {


        Path file = Paths.get(filePath, centroidFileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file.toFile()));
        try {

            Arrays.stream(initialCentroids).forEach(singleArray -> {

                try {

                    String line = Arrays.stream(singleArray).mapToObj(i -> String.valueOf(i)).collect(Collectors.joining("\t"));
                    System.out.println(line);
                    writer.append(line + "\n");
                    writer.flush();

                } catch (Exception ex) {

                    ex.printStackTrace();

                }


            });

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            writer.close();

        }


    }
}



