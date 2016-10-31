package com.ub.cse601.project2.KMeansMapReduce;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by nitish on 10/31/16.
 */
public class KMeansMR {

    private int initialCentroids;
    private String fileName;
    private String initialCentroidFileName;
    private int objectCount;
    private int attributeCount;
    private int clusterIndex;
    private double[][] dataMatrix;
    private Integer maxIterations;
    private Map<Integer, List<double[][]>> clusters;


    public KMeansMR(Integer initialCentroids, String fileName, Integer maxIterations) {

        this.initialCentroids = initialCentroids;
        this.fileName = fileName;
        this.maxIterations = maxIterations;

    }

    /*
    * This function picks a set of random initial centroids
     * from the original data-set and writes them to a new file.*/
    public void writeInitialCentroidsToFile(String filePath, String centroidFileName) throws Exception {

        try {

            double[][] kMeans = new double[initialCentroids][];

            int k = 0;

            Random rand = new Random();
            List<Integer> clusterIndices = new ArrayList<Integer>();
            Path file = Paths.get(filePath, centroidFileName);
            BufferedWriter writer = new BufferedWriter(new FileWriter(file.toFile()));

            //ch double count = 1;

            while (k < initialCentroids) {

                int centroidIndex = rand.nextInt(dataMatrix.length);

                if (clusterIndices.contains(centroidIndex)) {

                    continue;

                } else {

                    clusterIndices.add(centroidIndex);

                    kMeans[k] = Arrays.copyOfRange(dataMatrix[centroidIndex], 1, attributeCount + 1);

                    //kMeans[k][0] = count++;

                    Arrays.stream(kMeans[k]).forEach(x -> {
                        x = new BigDecimal(x).setScale(2, RoundingMode.HALF_UP).doubleValue();
                    });

                    k++;

                }

            }


            try {

                Arrays.stream(kMeans).forEach(singleArray -> {
                    try {

                        String line = Arrays.stream(singleArray).mapToObj(i -> String.valueOf(i)).collect(Collectors.joining("\t"));
                        System.out.println(line);

                        writer.append(line + "\n");
                        writer.flush();

                    } catch ( Exception ex ) {

                        ex.printStackTrace();

                    }


                });

            } catch ( Exception e ) {

                e.printStackTrace();

            } finally {

                writer.close();

            }


        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public void readGeneDataSet(String path) throws IOException {

        try {

            Path filePath = Paths.get(path, fileName);

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


    public static void main(String[] args) {

        try {

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter File name of data set: ");
            String fileName = sc.next();

            if (fileName == null || fileName.length() == 0) {

                fileName = "cho.txt";

            }

            String path = "data/";
            KMeansMR mapReduceObject = new KMeansMR(5, fileName, 100);
            mapReduceObject.readGeneDataSet(path);
            mapReduceObject.writeInitialCentroidsToFile(path, "initialCentroids.txt");

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

}
