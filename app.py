from flask import Flask, request, jsonify
from flask import  render_template

# -------- Spark imports  --------
import pyspark
import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from utils import *
from pyspark.sql import HiveContext
# -------- Spark imports  --------
#os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop"

spark = get_spark_Session()
print("Spark-Session Created!")
data_sources = spark.sql("show tables in default").toPandas()
data_sources = data_sources["tableName"].tolist()
print(data_sources)


app = Flask(__name__)

data = ["data1", "data2", "data3", "data4"]

graph = Graph()

@app.route('/')
def hello_world():
    global data_sources
    return render_template("ui.html", data=data_sources, nodes=graph.get_nodes())

@app.route('/add_source', methods=['POST'])
def addDataSource():
    id = request.form['id']
    node = Source(id)
    graph.add_node(node)
    return jsonify(node.get_Cyto_format())


@app.route('/get_node_ui', methods=['GET', 'POST'])
def get_node_ui():
    if request.method == 'POST':
        jsdata = request.form['data']
        print(jsdata)
    return "RPC-Flask"



if __name__ == '__main__':
    app.run()
