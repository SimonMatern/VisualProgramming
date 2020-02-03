from flask import Flask, request, jsonify
from flask import  render_template
import flask
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
    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps([])}

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

@app.route('/sqlFilterResponse', methods=['POST'])
def sqlFilterResponse():
    conditionsInput = json.loads(request.form['conditions'])
    id = request.form['id']
    node = graph[id]

    condition, label = createFilter(conditionsInput)
    df = node.df.where(condition)
    node = Node(label="Filter\n"+label,df=df,inputs=[node])
    graph.add_node(node)
    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps(node.get_Cyto_edges())}

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
    label = "Select " + ", ".join(columns)

    if len(columns)==len(rename):
        df = source.df.select(columns).toDF(*rename)
        label+= "\nAS " + ", ".join(rename)
    else:
        df = source.df.select(columns)
    node = Node(label=label,df=df,inputs=[source])
    graph.add_node(node)
    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps(node.get_Cyto_edges())}


@app.route('/sqlGetUnique', methods=['POST'])
def sqlGetUnique():
    id = request.form['id']
    column = request.form['column']
    node = graph[id]
    return jsonify(node.df.select(column).distinct().rdd.flatMap(lambda x: x).collect())


@app.route('/sqlGetRange', methods=['POST'])
def sqlGetRange():
    id = request.form['id']
    column = request.form['column']
    df = graph[id].df
    return jsonify(df.select(column).rdd.min()[0],df.select(column).rdd.max()[0])

@app.route('/sqlGetDateRange', methods=['POST'])
def sqlGetDateRange():
    id = request.form['id']
    column = request.form['column']
    df = graph[id].df.select(column)
    df = df.withColumn(column, df[column].cast(DateType()))
    dates = df.select(column).where(col(column).isNotNull())
    return jsonify(dates.rdd.min()[0],dates.rdd.max()[0])

@app.route('/showTable', methods=['POST'])
def showTable():
    id = request.form['id']
    df = graph[id].df
    df_pd = df.limit(50).toPandas()
    t = et.fromstring(df_pd.to_html())
    t.set('id', 'table')
    t.set("width", "100%")
    et.tostring(t)
    return et.tostring(t)

if __name__ == '__main__':
    app.run()
