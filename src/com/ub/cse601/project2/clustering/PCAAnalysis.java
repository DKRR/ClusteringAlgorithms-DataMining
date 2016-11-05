package com.ub.cse601.project2.clustering;

import com.sun.org.apache.regexp.internal.RE;
import com.sun.org.apache.xml.internal.security.utils.SignerOutputStream;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.correlation.Covariance;

import java.util.Arrays;

/**
 * Created by ramesh on 11/3/16.
 */
public class PCAAnalysis {


    public RealMatrix prepareFeatureMatrix(double[][] dataMatrix,int clusterIndex, int endIndex) {

        RealMatrix rm = MatrixUtils.createRealMatrix(dataMatrix);
        double[] geneIds = rm.getColumn(0);
        double[] clusterIds = rm.getColumn(clusterIndex);
        RealMatrix featureMatrix = rm.getSubMatrix(0, dataMatrix.length - 1, 1, endIndex);
        return featureMatrix;
    }

    public RealMatrix covarianceMatrix(RealMatrix featureMatrix){
        Covariance cv = new Covariance(featureMatrix);
        return cv.getCovarianceMatrix();
    }


    public RealMatrix performEigenDecomposition(RealMatrix covMatrix,RealMatrix featureMatrix){
        EigenDecomposition ed = new EigenDecomposition(covMatrix);
        RealMatrix eigenVectors = ed.getV();
        System.out.println("Eigen Values: "+Arrays.toString(ed.getRealEigenvalues()));
        RealMatrix principalComponents = featureMatrix.multiply(eigenVectors);
        return principalComponents;
    }

    public double[] findXScale(RealMatrix princiPalComponents){
        double[] scale = new double[2];
        scale[0] = princiPalComponents.getColumnVector(0).getMinValue();
        scale[1] = princiPalComponents.getColumnVector(0).getMaxValue();
        return scale;
    }
    public double[] findYScale(RealMatrix princiPalComponents){
        double[] scale = new double[2];
        scale[0] = princiPalComponents.getColumnVector(1).getMinValue();
        scale[1] = princiPalComponents.getColumnVector(1).getMaxValue();
        return scale;
    }


}
