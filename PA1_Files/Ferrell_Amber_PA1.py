from pyspark.sql import SparkSession
import sys


    

def main(sc):
    """Executes the main data processing and analysis pipeline
    
    Args:
        sc (SparkContext): The active Spark context

    Returns:
        dict: A dictionary containing intermediate RDDs, metrics, and component labels
    """
    
    input_path = "file:///" + sys.argv[1]
    rdd = sc.textFile(input_path)
    
    clean_edges = rdd.filter(lambda x: not x.startswith('#')) \
        .map(lambda x: tuple(map (int, x.split('\t')))) \
        .filter(lambda x: x[0] != x[1]) \
        .flatMap(lambda x: [x, (x[1], x[0])])
  
    unique_edges = clean_edges.distinct().persist()
    edge_group = unique_edges.groupByKey()
    degrees = edge_group.map(lambda x: (x[0], len(x[1])))
    
    total_lines = rdd.count()
    edge_count = unique_edges.count()  
    
    # run label prop to get component labels: (node_id, component_id)
    component_labels = label_prop(edge_group)
    
    #TODO: find size of largest component, flip to (component_id, 1) and sum
    #TODO: find max component size
    
    return {
        "clean_edges": clean_edges,
        "unique_edges": unique_edges,
        "edge_group": edge_group,
        "degrees": degrees,
        "total_lines": total_lines,
        "edge_count": edge_count,
       # "largest_component_size": largest_component_size
    }
    

def label_prop(edge_group, max_iterations=15):
    """Propagates vertex labels iteratively to determine weakly connected components.

    Args:
        edge_group (RDD): An RDD of adjacency lists in the format (node_id, iterable of neighbors)
        
        max_iterations (int): The maximum number of message-passing rounds to perform

    Returns:
        RDD: An RDD containing final component assignments as (node_id, component_id) pairs
    """
    
    labels = edge_group.map(lambda x: (x[0], x[0]))
    
    for _ in range(max_iterations):
        joined = edge_group.join(labels)
        messages = joined.flatMap(
            lambda x: [(x[0], x[1][1])]
            + [(neighbor, x[1][1]) for neighbor in x[1][0]]
    )
        labels = messages.reduceByKey(min)
    return labels




if __name__ == "__main__":
    print("I am ready")
    
    spark = SparkSession.builder \
        .appName("PA1_Task1") \
        .getOrCreate()
    sc = spark.sparkContext
    metrics = main(sc)
    print(
        f"\n===================================\n",
        "first 5 lines of cleaned data: \n\n",
        metrics["clean_edges"].take(5),
        "\n===================================\n",
        "first 5 unique bi-edges: \n\n",
        metrics["unique_edges"].take(5),
        "\n=================================== \n Task one metrics: \n\n",
        f" - Total lines: {metrics['total_lines']} \n",
        f" - total unique bidirectional edges: {metrics['edge_count']} \n",
        "\n===================================\n first 5 adjacency lists: \n \n",
        metrics["edge_group"].map(lambda x: (x[0], list(x[1]))).take(5), " \n ",
        "\n===================================\n first 5 node degrees: \n\n",
        metrics["degrees"].take(5),
        "\n===================================\n"#,
        #f"Largest WCC node count: {metrics['largest_component_size']} \n",
       # "===================================\n"
    )
    
    