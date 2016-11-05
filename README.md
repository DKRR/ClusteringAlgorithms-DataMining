# ClusteringAlgorithms-DataMining Project2

Step to run Project Components

Part-1: Running Clustering Algorithms

1. Extract project2.jar from Project.zip to any location on your system.
2. create data folder in same directory as project2.jar and add data files you want to run(ex cho.txt, iyer.txt)
3. Give the following commands to run specific clustering algorithm
    1. KMeans
     java -cp project2.jar com.ub.cse601.project2.run.RunKMeans
    2. Hierarchical Clustering
     java -cp project2.jar com.ub.cse601.project2.run.RunHierarchialClustering
    3. DBScan
    java -cp project2.jar com.ub.cse601.project2.run.RunDBScan
    

Part-2: Running KMeans Map Reduce

1. Follow steps 1 and 2 from part1.
2. create two new folders input and centroids inside data folder created in above step.
3. Give the following commands to run KMeans MR
   java -cp project2.jar com.ub.cse601.project2.run.RunKMeansMR

