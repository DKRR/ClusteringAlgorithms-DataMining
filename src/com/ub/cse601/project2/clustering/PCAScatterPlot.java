package com.ub.cse601.project2.clustering;

import com.amazonaws.services.opsworks.model.App;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.util.*;
import java.util.stream.Collectors;


public class PCAScatterPlot extends Application {

    public static double[][] dataMatrix = null;
    public static String plotTitle = null;
    public static double[] scaleX = null;
    public static double[] scaleY = null;
    public static Integer clusterIndex = null;

    public static void launchClass(double[][] dataMatrix, String plotTitle, double[] scaleX, double[] scaleY, Integer clusterIndex){
        PCAScatterPlot.dataMatrix = dataMatrix;
        PCAScatterPlot.plotTitle = plotTitle;
        PCAScatterPlot.scaleX = scaleX;
        PCAScatterPlot.scaleY = scaleY;
        PCAScatterPlot.clusterIndex = clusterIndex;
        Application.launch(PCAScatterPlot.class,plotTitle);
    }
    @Override
    public void start(Stage stage) {
        stage.setTitle(plotTitle+" Scatter plot");
        final NumberAxis xAxis = new NumberAxis(scaleX[0], scaleX[1], 0.01);
        final NumberAxis yAxis = new NumberAxis(scaleY[0],scaleY[1],0.01);
        final ScatterChart<Number, Number> sc = new
                ScatterChart<Number, Number>(xAxis, yAxis);
        sc.setTitle(plotTitle+" Cluster Analysis");
        Map<Double, List<double[]>> clusterMap = new TreeMap<Double, List<double[]>>(Arrays.stream(dataMatrix).filter(x -> x[clusterIndex] != Double.NEGATIVE_INFINITY).collect(
                Collectors.groupingBy(x -> x[clusterIndex], Collectors.mapping(x -> x, Collectors.toList()))));
        clusterMap.forEach((x, y) -> {
            ScatterChart.Series series = new ScatterChart.Series();
            series.setName("Cluster "+x.intValue());
            for (double[] point : y) {
                series.getData().add(new ScatterChart.Data<>(point[1], point[2]));
            }
            sc.getData().add(series);
        });
        Scene scene  = new Scene(sc, 700, 500);
        stage.setScene(scene);
        stage.show();
    }
}