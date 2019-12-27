from pyspark.sql import SparkSession
from pyspark import SparkConf
from os.path import expanduser, join, abspath
import os

def get_spark_Session():
    os.environ["SPARK_HOME"]="/home/nodeuser/nfs_share/spark-2.4.4-bin-hadoop2.7"
    warehouse_location = abspath('/user/hive/warehouse')
    #sc = SparkContext(master = "yarn-client")


    sconf = SparkConf().setAll([('master','yarn'),('deploy-mode','client'),('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'),
                                ('spark.driver.memory','4g'),('spark.executor.instances', 10), ('spark.sql.warehouse.dir', warehouse_location)])
    spark = SparkSession \
        .builder \
        .config(conf=sconf) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

spark = get_spark_Session()

class Node:
    def __init__(self, id, label, inputs=None):

        self.id = id
        self.label = label
        self.inputs = inputs

    def get_Cyto_format(self):
        return {"id":self.id , "label":self.label}

class Source(Node):
    def __init__(self, id):
        self.df = spark.sql("select * from " + str(id))
        super().__init__(id=id,label="Datenquelle: " + str(id))


class Graph:
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def add_node(self, node : Node):
        self.nodes[node.id] = node

    def get_nodes(self):
        nodes = []
        for node in self.nodes.values():
            nodes.append(node.get_Cyto_format())
        return nodes