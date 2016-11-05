package com.ub.cse601.project2.run;

/**
 * Created by nitish on 11/2/16.
 */

import com.ub.cse601.project2.hadoop.KMeansMRStarter;

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
import java.util.concurrent.atomic.AtomicInteger;
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
    private List<Integer> geneIds;


    public RunKMeansMR(Integer initialCentroids, String fileName, Integer maxIterations, List<Integer> geneIds) {
        this.initialCentroids = initialCentroids;
        this.fileName = fileName;
        this.maxIterations = maxIterations;
        this.geneIds = geneIds;
    }


    public static void main(String[] args) throws Exception {

        try {

            Scanner sc = new Scanner(System.in);
            System.out.println("Enter File name of data set: ");
            String fileName = sc.nextLine();
            if (fileName == null || fileName.length() == 0) fileName = "cho.txt";
            String path = "data/";
            System.out.println("Do you want to input initial centroids, Please type 'Y' or 'N': ");
            String manual = sc.nextLine();
            List<Integer> geneIds = new ArrayList<Integer>();
            int k = 0;
            if (manual.equalsIgnoreCase("y")) {
                System.out.println("Enter comma separated gene id's: ");
                String genes = sc.nextLine().replaceAll("\\s+", "");
                System.out.println(genes);
                geneIds = Arrays.stream(genes.split(",")).collect(Collectors.mapping(x -> Integer.parseInt(x.trim()), Collectors.toList()));
                System.out.println(geneIds);
            } else {
                System.out.println("Enter No. of Clusters: ");
                k = sc.nextInt();
            }
            System.out.println("Enter Max Iterations: ");
            Integer maxIter = sc.nextInt();
            //System.out.println("args" + args.length);

            //String inPath = !args.equals("") && args[0] != null || args[0].length() > 0 ? args[0] : "data/input/";
            //String outPath = !args.equals("") && args[1] != null || args[1].length() > 0 ? args[1] : "data/output/";

            String inPath = "data/input/";
            String outPath = "data/output/";
            String centroidPath = "data/centroids/";
            String dataPath = "data/";


            Files.list(Paths.get(centroidPath)).forEach(x -> {

                try {

                    Files.delete(x);

                } catch (Exception e) {

                    e.printStackTrace();

                }
            });
            Files.list(Paths.get(inPath)).forEach(x -> {

                try {

                    Files.delete(x);

                } catch (Exception e) {

                    e.printStackTrace();

                }
            });

            Files.copy(Paths.get(dataPath, fileName), Paths.get(inPath, fileName));
            RunKMeansMR kMeansMR = new RunKMeansMR(k, fileName, maxIter, geneIds);
            double[][] returnedDataMatrix = kMeansMR.readGeneDataSet(inPath);
            double[][] initialKMeans = kMeansMR.initKMeans();
            if (manual.equalsIgnoreCase("Y")) {
                System.out.println("Initial chosen centroids are:");
            } else {
                System.out.println("Randomly chosen initial centroids:");
            }
            Arrays.stream(initialKMeans).forEach(x -> {
                System.out.println(Arrays.toString(x));
            });
            /*double[][] tk = new double[5][];
            tk[0] = new double[]{-0.69, -0.96, -1.16, -0.66, -0.55, 0.12, -1.07, -1.22, 0.82, 1.4, 0.71, 0.68, 0.11, -0.04, 0.19, 0.82};
            tk[1] = new double[]{-0.2, 0.14, 0.73, 0.3, -0.28, -0.12, -0.27, -0.22, -0.25, 0.24, 0.07, 0.0, -0.1, -0.06, -0.17, -0.08};
            tk[2] = new double[]{-0.32, -0.21, 1.11, 0.84, -0.14, -0.09, -0.37, -0.59, -0.4, 0.44, 0.25, 0.04, 0.08, -0.304, -0.39, -0.36};
            tk[3] = new double[]{-0.48, 0.06, -0.01, 0.31, 0.37, 0.27, 0.35, 0.31, -0.19, -0.27, -0.23, 0.15, 0.024, 0.18, -0.24, -0.41};
            tk[4] = new double[]{-0.79, -0.56, -0.79, -0.23, -0.53, -0.14, 0.61, 0.95, 0.96, 0.38, -0.11, -0.31, -0.41, 0.49, 0.08, 0.15};*/
            kMeansMR.writeInitialCentroidsToFile(centroidPath, "centroids_0.txt", initialKMeans);
            KMeansMRStarter startJob = new KMeansMRStarter(inPath, outPath, maxIter, returnedDataMatrix, kMeansMR.clusterIndex);
            startJob.runKMeansMR();

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public double[][] readGeneDataSet(String path) throws IOException {

        try {

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

        return dataMatrix;

    }

    public double[][] initKMeans() {

        double[][] kMeans = null;
        if (geneIds.size() > 0) {
            kMeans = new double[geneIds.size()][];
            for (int i = 0; i < geneIds.size(); i++) {
                kMeans[i] = Arrays.copyOfRange(dataMatrix[geneIds.get(i) - 1], 1, attributeCount + 1);
                Arrays.stream(kMeans[i]).forEach(x -> {
                    x = new BigDecimal(x).setScale(2, RoundingMode.HALF_UP).doubleValue();
                });
            }
        } else {
            kMeans = new double[initialCentroids][];
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
        }
        return kMeans;
    }

    public void writeInitialCentroidsToFile(String filePath, String centroidFileName, double[][] initialCentroids) throws Exception {


        Path file = Paths.get(filePath, centroidFileName);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file.toFile()));
        AtomicInteger counter = new AtomicInteger(0);
        try {

            Arrays.stream(initialCentroids).forEach(singleArray -> {

                try {

                    String line = Arrays.stream(singleArray).mapToObj(i -> String.valueOf(i)).collect(Collectors.joining("\t"));
                    if (counter.get() != (initialCentroids.length - 1)) {

                        writer.append(line + "\n");

                    } else {

                        writer.append(line);

                    }

                    writer.flush();
                    counter.getAndIncrement();

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



