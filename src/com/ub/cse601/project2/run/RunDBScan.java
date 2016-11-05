package com.ub.cse601.project2.run;

import com.ub.cse601.project2.clustering.DBScan;
import com.ub.cse601.project2.clustering.KMeans;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by VenkataRamesh on 11/2/2016.
 */
public class RunDBScan {

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter File name of data set: ");
        String fileName = sc.next();
        double episilon = 0;
        int minPoints = 0;
        System.out.println("Enter min threshold points for DBScan as positive integer: ");
        while (minPoints == 0) {
            try {
                minPoints = sc.nextInt();
                break;
            } catch (NumberFormatException ex) {
                System.out.println("Incorrect value, Enter min threshold points for DBScan as positive integer: ");
            }

        }
        System.out.println("Enter episilon value as positive real number: ");
        while (episilon == 0) {
            try {
                episilon = sc.nextDouble();
                break;
            } catch (NumberFormatException ex) {
                System.out.println("Incorrect value, Enter episilon value as positive real number: ");
            }

        }
        if (fileName == null || fileName.length() == 0) fileName = "cho.txt";
        String path = "data/";
        DBScan dbScan = new DBScan(fileName, minPoints, episilon);
        dbScan.readGeneDataSet(path);
        double[][] distMat = dbScan.calculateDistanceMatrix();
        /*Arrays.stream(distMat).forEach(x->{
            System.out.println(Arrays.toString(x));
        });*/
        dbScan.runDBScan();
        //dbScan.printClusters();

    }
}
