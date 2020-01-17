from flask import Flask, request, jsonify
from flask import  render_template
import json
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
    return render_template("ui.html", data=data_sources, nodes=graph.get_nodes(), edges=graph.get_edges())

@app.route('/addDataSource', methods=['POST'])
def addDataSource():
    id = request.form['id']
    node = Source(id)
    graph.add_node(node)
    return jsonify(node.get_Cyto_node())


@app.route('/sqlFilter', methods=['POST'])
def sqlFilter():
    id = request.form['id']
    print(id)
    node = graph[id]
    columns = node.df.schema.names
    data_types = [field.dataType.simpleString() for field in node.df.schema.fields]
    print(data_types)
    dictionary = dict(zip(columns, data_types))
    return dictionary

@app.route('/sqlSelect', methods=['POST'])
def sqlSelect():
    id = request.form['id']
    print(id)
    node = graph[id]
    return jsonify(node.df.schema.names)

@app.route('/sqlSelectResponse', methods=['POST'])
def sqlSelectResponse():
    columns = eval(request.form['columns'])
    rename = eval(request.form['rename'])
    print(columns)
    print(rename)
    id = request.form['id']
    source = graph[id]
    df = None
    if len(columns)==len(rename):
        df = source.df.select(columns).toDF(*rename)
    else:
        df = source.df.select(columns)
    node = Node(label="Select",df=df,inputs=[source])
    graph.add_node(node)
    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps(node.get_Cyto_edges())}

@app.route('/get_node_ui', methods=['GET', 'POST'])
def get_node_ui():
    if request.method == 'POST':
        jsdata = request.form['data']
    return "RPC-Flask"



if __name__ == '__main__':
    app.run()
