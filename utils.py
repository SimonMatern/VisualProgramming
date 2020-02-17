from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row


from functools import reduce
from operator import and_, or_
from pyspark.sql.types import DateType
from xml.etree import ElementTree as et
import os
from os.path import expanduser, join, abspath
from datetime import datetime


from flask import Flask, request, jsonify, render_template

# -------- Bookeh imports  --------
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models.sources import AjaxDataSource
# --------END Bokeh imports  --------
import uuid

def get_spark_Session():
    os.environ["SPARK_HOME"] = "/home/nodeuser/nfs_share/spark-2.4.4-bin-hadoop2.7"
    os.environ["PYSPARK_PYTHON"] = "/usr/local/anaconda3/bin/python"
    os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop/"
    os.environ["HADOOP_OPTS"] = "-Djava.library.path=/usr/local/hadoop//lib"
    os.environ["LD_LIBRARY_PATH"] = "/usr/local/hadoop/lib/native/"
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

    print(os.environ)
    warehouse_location = abspath('/user/hive/warehouse')
    # sc = SparkContext(master = "yarn-client")

    #TODO: Change to Yarn-Client
    sconf = SparkConf().setAll([('spark.master', 'local'),
                                ('spark.deploy-mode', 'client'),
                                ('spark.executor.memory', '4g'),
                                ('spark.app.name', 'Flask-Spark'),
                                ('spark.executor.cores', '4'),
                                ('spark.driver.memory', '4g'),
                                ('spark.executor.instances', 10),
                                ('spark.sql.warehouse.dir', warehouse_location)])
    spark = SparkSession \
        .builder \
        .config(conf=sconf) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.addFile("utils.py")

    ssc = StreamingContext(spark.sparkContext, 1)
    return spark, ssc

spark, _ = get_spark_Session()


class Node:
    def __init__(self, label, df=None, inputs=None):
        self.id = uuid.uuid1().hex
        self.label = label
        self.inputs = inputs
        self.df = df
        self.type= "static"


    def get_Cyto_node(self):
        return {"id": self.id, "label": self.label}

    def get_Cyto_edges(self):
        if not self.inputs:
            return []
        return [{"id": node.id + "-" + self.id, "source": node.id, "target": self.id} for node in self.inputs]


class Source(Node):
    def __init__(self, label):
        super().__init__(label="Hive: " + str(label))
        self.df = spark.sql("select * from " + str(label))

class StreamingSource(Node):
    def __init__(self, id , ssc):
        super().__init__(label="Kafka: " + str(id))

        self.ssc = ssc
        stream = KafkaUtils.createDirectStream(ssc, [id], {'bootstrap.servers': 'cluster0309:9094',
                                                                    'auto.offset.reset': 'largest',
                                                                    'group.id': 'spark-group'})
        self.type= "stream"
        self.stream = stream.map(lambda x: filterDict(eval(x[1]), isNumerical))

class StreamingNode(Node):
    def __init__(self, label, stream=None, inputs=None, ssc=None):
        super().__init__(label=label, inputs=inputs)
        self.stream = stream
        self.ssc = ssc




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

def reduceSum(x,y):
    """
    Sum two elements in the Stream.
    :param x: tuple or dictionary
    :param y: tuple or dictionary
    :return:
    """
    if(type(x)==dict and type(y)==dict):
        x = filterDict(x, isNumerical)
        y = filterDict(y, isNumerical)

        return{ k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y) }


def filterDict(dictObj, filterCallback):
    """
    This funciton filters a dicitonary based on a generic boolean filter.
    :param dictObj: A dictionary
    :param filterCallback: a boolean callback that filter the dictionary. Takes key and value as parameters.
    :return:
    """
    newDict = dict()
    # Iterate over all the items in dictionary
    for (key, value) in dictObj.items():
        # Check if item satisfies the given condition then add to new dict
        if filterCallback(key, value):
            newDict[key] = value
    return newDict

def isNumerical(key,value):
    return type(value) in [float, int]


def countStream(x):
    x["count"] = 1
    return x


def reduceSum(x, y):
    if (type(x) == dict and type(y) == dict):
        x = filterDict(x, isNumerical)
        y = filterDict(y, isNumerical)

        return {k: x.get(k, 0) + y.get(k, 0) for k in set(x) | set(y)}


def count_to_mean(x):
    for key in x:
        x[key] = x[key] / x["count"]
    del x["count"]
    return x

def AverageAndStd(time, rdd, streaming_dict, id):
    global x
    if rdd.isEmpty():
        return
    df = rdd.map(lambda x: Row(**x)).toDF()
    columns = df.schema.names
    conditions_mean = [_mean(col(column)).alias(column +"_mean") for column in columns]
    conditions_std = [_stddev(col(column)).alias(column +"_stddev") for column in columns]

    df = df.select(conditions_mean+conditions_std).toPandas()
    df["time_stamp"]= time.timestamp() * 1000

    if id in streaming_dict:
        streaming_dict[id]= streaming_dict[id].append(df, ignore_index=True)
    else:
        streaming_dict[id]=df


def make_line_plot(dictionary, id):
    source = AjaxDataSource(data_url=request.url_root + 'data/' + id +"/",
                            polling_interval=2000, mode='replace')
    source.data = dictionary
    plot = figure(plot_height=300, sizing_mode='scale_width',x_axis_type="datetime")

    for key in dictionary.keys():
        if key == "time_stamp": continue
        plot.line('time_stamp', key, source=source, line_width=4)

    script, div = components(plot)
    return script, div


def df_to_dict(df):
    new_dict = dict()
    for key in df:
        new_dict[key] = df[key].to_list()

    return new_dict