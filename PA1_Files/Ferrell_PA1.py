from pyspark.sql import SparkContext, SparkConf, SparkSession

conf = SparkConf().setAppName("MySparkApp").setMaster("yarn")
sc = SparkContext(conf=conf)

def main(sc):
    
    # load the text file 
    rdd = sc.textFile("file:///s/chopin/n/under/C837364858/cs435/Lab3_PA1/PA1/com-dblp.ungraph.txt")
    
    # pre-processing, filtering out header and comment lines with a hash
    clean_lines = rdd.filter(lambda x: not x.startswith('#'))
    
    # transform strings into int pairs soruce and destination
    # edges = clean_lines.map(lambda x: tuple(map(int, x.split('\t'))))
    
    # remove self loops where source equals destination
    # no_selfies = edges.filter(lambda x: x[0] != x[1])
    
    no_selfies = clean_lines.map(lambda x: tuple(map (int, x.split('\t')))).filter(lambda x: x[0] != x[1])
    
    #ensure bidrectionality
    bi_edges = no_selfies.flatMap(lambda x: [x, (x[1], x[0])])
    
    # remove duplicate entires using distinct() 
    unique_edges = bi_edges.distinct().persist()
    
    # grouping => edge_group : (author, neighbors_iterable)
    edge_group = unique_edges.groupByKey()
    
    # testing
    print("===================================\n", "first 5 lines of cleaned data: \n", "***************************************************** \n OUTPUT \n" , no_selfies.take(5),"\n ***************************************************** \n ")
    
    print("===================================\n","first 5 unique bi-edges: \n","***************************************************** \n OUTPUT \n",unique_edges.take(5), "\n ***************************************************** \n ")
    
    total_lines = rdd.count()
    #self_loop_count = edges.filter(lambda x: x[0] == x[1]).count()
    self_loop_count = no_selfies.filter(lambda x: x[0] == x[1]).count()
    edge_count = unique_edges.count()
    print(f"**************************************** \n Task one metrics: \n - Total lines: {total_lines} \n - self-loops removed: {self_loop_count} \n - total unique bidirectional edges: {edge_count} \n *************************************" )
    
if __name__ == "__main__":
    print("I am ready")
    
    spark = SparkSession.builder \
        .appName("PA1_Task1") \
        .getOrCreate()
    sc = spark.sparkContext
    
    main(sc)