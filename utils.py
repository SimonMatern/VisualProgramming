from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, lit
from os.path import expanduser, join, abspath
from functools import reduce
from operator import and_, or_
from pyspark.sql.types import DateType
from xml.etree import ElementTree as et


import os
import uuid

def get_spark_Session():
    os.environ["SPARK_HOME"] = "/home/nodeuser/nfs_share/spark-2.4.4-bin-hadoop2.7"
    os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda3/bin/python"
    os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop/"
    os.environ["HADOOP_OPTS"] = "-Djava.library.path=/usr/local/hadoop//lib"
    os.environ["LD_LIBRARY_PATH"] = "/usr/local/hadoop/lib/native/"

    print(os.environ)
    warehouse_location = abspath('/user/hive/warehouse')
    # sc = SparkContext(master = "yarn-client")

    #TODO: Change to Yarn-Client
    sconf = SparkConf().setAll([('spark.master', 'spark://master:7077'), ('spark.deploy-mode', 'client'), ('spark.executor.memory', '4g'),
                                ('spark.app.name', 'Flask-Spark'), ('spark.executor.cores', '4'),
                                ('spark.driver.memory', '4g'), ('spark.executor.instances', 10),
                                ('spark.sql.warehouse.dir', warehouse_location)])
    spark = SparkSession \
        .builder \
        .config(conf=sconf) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark

spark = get_spark_Session()


class Node:
    def __init__(self, label, df=None, inputs=None):
        self.id = uuid.uuid1().hex
        self.label = label
        self.inputs = inputs
        self.df = df

    def get_Cyto_node(self):
        return {"id": self.id, "label": self.label}

    def get_Cyto_edges(self):
        if not self.inputs:
            return []
        return [{"id": node.id + "-" + self.id, "source": node.id, "target": self.id} for node in self.inputs]


class Source(Node):
    def __init__(self, label):
        super().__init__(label="Datenquelle: " + str(label))
        self.df = spark.sql("select * from " + str(label))


class Graph:
    def __init__(self):
        self.nodes = {}
        self.edges = {}

    def __getitem__(self, id):
        return self.nodes[id]

    def add_node(self, node: Node):
        self.nodes[node.id] = node

    def get_nodes(self):
        nodes = []
        for node in self.nodes.values():
            nodes.append(node.get_Cyto_node())
        return nodes

    def get_edges(self):
        edges = []
        for node in self.nodes.values():
            edges += node.get_Cyto_edges()
        return edges


def createFilter(conditionParams):
    conditions = [createCondition(condition) for condition in conditionParams]
    conditions, labels = zip(*conditions)
    print(labels)
    return reduce(or_, conditions), '\n'.join(labels)


def createCondition(condition):
    if condition["type"] == "string" and condition["option"] == "RegEx":
        return col(condition["column"]).rlike(condition["condition"]), \
               condition["column"] + " RegExp " + "\"" + condition["condition"] + "\""

    if condition["type"] == "numerical":
        print(condition)

        cond1, cond2 = lit(True), lit(True)
        label_lower = ""
        label_upper = ""

        if condition["lowerBound"] != "":
            if condition["lowerBoundType"] == "[":
                cond1 = col(condition["column"]) >= eval(condition["lowerBound"])
                label_lower = condition["column"] + " >= " + condition["lowerBound"]
            else:
                cond1 = col(condition["column"]) > condition["lowerBound"]
                label_lower = condition["column"] + " > " + condition["lowerBound"]

        if condition["upperBound"] != "":
            if condition["upperBoundType"] == "]":
                cond2 = col(condition["column"]) <= condition["upperBound"]
                label_upper = condition["column"] + " <= " + condition["upperBound"]

            else:
                cond2 = col(condition["column"]) < condition["upperBound"]
                label_upper = condition["column"] + " < " + condition["upperBound"]

        label = ""
        if label_upper == "":
            label = label_lower
        elif label_lower == "":
            label = label_upper
        else:
            label = label_lower + " & " + label_upper
        return and_(cond1, cond2), label

    if condition["type"] == "selectize":
        selection = condition["list"]
        column = condition["column"]

        filter_condition = col(column).isin(selection)
        selection = map(str,selection)
        label = column + " IN " + ", ".join(selection)
        return filter_condition, label

    if condition["type"]=="rangeDate":
        filter_condition = (col(condition["column"]) >= condition["lowerBound"])  &  (col(condition["column"]) <= condition["upperBound"])
        label = condition["column"] + " >= " + condition["lowerBound"] + " & " + condition["column"] + " <= " + condition["upperBound"]
        return filter_condition, label