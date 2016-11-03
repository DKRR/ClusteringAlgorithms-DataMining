package com.ub.cse601.project2.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        private List<Double[]> centroidList = new ArrayList<Double[]>();
        private ArrayList<Double[]> geneIndexExpList = new ArrayList<Double[]>();
        private int centroidLength = 0;


        protected void setup(Context context) throws IOException, InterruptedException {

            try {


                super.setup(context);
                Configuration conf = context.getConfiguration();

                String filePath = "data/input/";
                String fileName = "initialCentroids.txt";
                Path centroidFilePath = Paths.get(filePath, fileName);

                List<String> centroidData = Files.readAllLines(centroidFilePath, StandardCharsets.UTF_8);
                //TODO: check whether you need to remove last row

                for ( String singleGeneString : centroidData ) {

                    if ( singleGeneString != null || singleGeneString != "" ) {

                        String[] centroidExpressions = singleGeneString.split("\t");
                        centroidLength = centroidExpressions.length;
                        //System.out.println("length =" + centroidExpressions.length);
                        Double[] singleCentroidExpressionStore = new Double[singleGeneString.split("\t").length];

                        for ( int i = 0; i < centroidExpressions.length; i++ ) {

                            singleCentroidExpressionStore[i] = Double.valueOf(centroidExpressions[i]);

                        }

                        centroidList.add(singleCentroidExpressionStore);

                    }

                }
                //Double[] newCentroidExp = new Double[centroidLength];

            } catch ( Exception e ) {

                e.printStackTrace();

            }

        }


        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            /*String[] tempLength = value.toString().split("\t");
            String tempStr = value.toString();
            System.out.println("value is "+value);
            int tlen = tempLength.length;
            System.out.println("len is "+tlen);*/

            //int genesLength = (value.toString().split("\t").length) - 2;
            //System.out.println(genesLength);
            //String reduceKeyString = key.toString();
            //System.out.println("key value string =" + reduceKeyString);
            //Double[] newCentroidExp = new Double[reduceKeyString.split("\t").length];
            Double[] newCentroidExp = new Double[centroidLength];
            Arrays.fill(newCentroidExp, 0.00);

            int genesLength = centroidLength;
            System.out.println(genesLength);

            int genesCount = 0;
            List<Integer> clusterGenePoints = new ArrayList<Integer>();

            for (Text eachValue: value) {


                genesCount = genesCount + 1;
                String singleGeneExpression = eachValue.toString();
                String[] singleGeneExpSplit = singleGeneExpression.split("\t");
                //System.out.println(Arrays.toString(singleGeneExpSplit));

                //System.out.println("singleGeneExpSplit[0] = " + singleGeneExpSplit[0]);
                clusterGenePoints.add(Integer.parseInt(singleGeneExpSplit[0]));
                Double[] singleGeneDouble = new Double[singleGeneExpression.split("\t").length];

                //System.out.println("singlgenelength =" + singleGeneExpSplit.length);

                for ( int i = 0; i < singleGeneExpSplit.length; i++) {

                    singleGeneDouble[i] = Double.valueOf(singleGeneExpSplit[i]);
                    //System.out.println("data exps = " + singleGeneDouble[i]);

                    if(i>1){
                        //System.out.println("newcentroid value = " + newCentroidExp[i-2]);
                        newCentroidExp[i-2] = newCentroidExp[i-2] + Double.valueOf(singleGeneExpSplit[i]);
                    }


                }

            }

            for(int i = 0; i < genesLength; i ++){

                newCentroidExp[i] = (double)newCentroidExp[i]/genesCount;
            }

            String filePath = "data/";
            String fileName = "initialCentroids.txt";
            Path centroidFilePath = Paths.get(filePath, fileName);

            //Files.createFile(centroidFilePath);
            String strLine = Arrays.stream(newCentroidExp).map(Object::toString).collect(Collectors.joining("\t"));
            Files.write(centroidFilePath, strLine.getBytes(), StandardOpenOption.CREATE_NEW);

            context.write(new Text(Arrays.toString(newCentroidExp)), new Text(clusterGenePoints.stream().map(Object::toString).collect(Collectors.joining(","))));

        }

    }