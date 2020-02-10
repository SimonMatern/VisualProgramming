from flask import Flask, request, jsonify, render_template

import flask
import json
# -------- Spark imports  --------
import pyspark
import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from utils import *
from pyspark.sql import HiveContext
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col

# --------END Spark imports  --------

# -------- Bookeh imports  --------
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models.sources import AjaxDataSource
# --------END Bokeh imports  --------

from bokeh.models.sources import AjaxDataSource


from pykafka import KafkaClient

#os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop"

spark, ssc = get_spark_Session()
print("Spark-Session Created!")
data_sources = spark.sql("show tables in default").toPandas()
data_sources = data_sources["tableName"].tolist()
print(data_sources)

HOSTS = "cluster0309:9094"
client = KafkaClient(HOSTS)
topics = [topic.decode("utf-8") for topic in list(client.topics.keys())]



app = Flask(__name__)

data = ["data1", "data2", "data3", "data4"]

graph = Graph()

@app.route('/')
def hello_world():
    global data_sources
    return render_template("ui.html", tables=data_sources, topics=topics, nodes=graph.get_nodes(), edges=graph.get_edges())

@app.route('/addDataSource', methods=['POST'])
def addDataSource():
    id = request.form['id']
    node = Source(id)
    graph.add_node(node)
    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps([])}

@app.route('/addStreamingDataSource', methods=['POST'])
def addStreamingDataSource():
    id = request.form['id']
    node = StreamingSource(id)
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



def processRow(row):
    print("Bla")

def AverageAndStd(time, rdd):
    if rdd.isEmpty():
        return
    print(time)

@app.route('/vizualizeStream', methods=['POST'])
def vizualizeStream():
    id = request.form['id']
    source = graph[id]
    stream = source.stream



    stream.map(lambda x: filterDict(eval(x[1]), isNumerical)).foreachRDD(processRow)
    ssc = source.ssc
    node = StreamingNode(label="Visualize",stream=stream,inputs=[source])
    graph.add_node(node)
    ssc.start()
    ssc.awaitTermination()

    return {"node":json.dumps(node.get_Cyto_node()),"edges":json.dumps([])}


def make_line_plot(dictionary, id):
    source = AjaxDataSource(data_url=request.url_root + 'data/' + id +"/",
                            polling_interval=2000, mode='replace')
    source.data = dictionary
    plot = figure(plot_height=300, sizing_mode='scale_width')

    for key in dictionary.keys():
        plot.line('x', key, source=source, line_width=4)

    script, div = components(plot)
    return script, div


plots = []
@app.route('/dashboard/')
def show_dashboard():
    global plots
    if not plots:
        plots.append(make_line_plot(dict(x=[], y=[]), id="bla"))
    return render_template('plots.html', plots=plots)


streamingData = dict()
@app.route('/data/<id>/', methods=['POST'])
def data(id):
    global streamingData

    return jsonify(x=[x], y=[y])



    global streamingData
    streamingData[id] = df
    print(df)


if __name__ == '__main__':
    app.run()
