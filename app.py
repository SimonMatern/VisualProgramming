from flask import Flask, request, jsonify, render_template, redirect, url_for
import os

import json
import time
import csv

# -------- Spark imports  --------
from pyspark.sql import Row
from utils import *
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
# --------END Spark imports  --------

# -------- Bookeh imports  --------
from bokeh.plotting import figure
from bokeh.embed import components
# --------END Bokeh imports  --------

import sqlalchemy as sql
import pandas as pd



from pykafka import KafkaClient

# os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop"
app = Flask(__name__)

spark = get_spark_Session()
spark.sparkContext.addFile("utils.py")

ssc = StreamingContext(spark.sparkContext, 1)

print("Spark-Session Created!")
# data_sources = spark.sql("show tables in default").toPandas()
# data_sources = data_sources["tableName"].tolist()
# print(data_sources)

# List all Hive Databases and their content
dbs = spark.sql("show databases").toPandas()
dbs_names = dbs["databaseName"].tolist()
data_sources = {db : spark.sql("show tables in " +str(db)).toPandas()["tableName"].tolist()  for db in dbs_names}



HOSTS = "cluster0101:9094"
client = KafkaClient(HOSTS)
topics = [topic.decode("utf-8") for topic in list(client.topics.keys())]

data = ["data1", "data2", "data3", "data4"]

graph = Graph()


@app.route('/')
def hello():
    global data_sources
    return render_template("ui.html", tables=data_sources, topics=topics, nodes=graph.get_nodes(),
                           edges=graph.get_edges())


@app.route('/addDataSource', methods=['POST'])
def addDataSource():
    id = request.form['id']
    node = Source(id)
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps([])}


@app.route('/sqlFilter', methods=['POST'])
@app.route('/getColumns', methods=['POST'])
def getColumns():
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
    node = Node(label="Filter\n" + label, df=df, inputs=[node])
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}


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

    if len(columns) == len(rename):
        df = source.df.select(columns).toDF(*rename)
        label += "\nAS " + ", ".join(rename)
    else:
        df = source.df.select(columns)
    node = Node(label=label, df=df, inputs=[source])
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}


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
    return jsonify(df.select(column).rdd.min()[0], df.select(column).rdd.max()[0])


@app.route('/sqlGetDateRange', methods=['POST'])
def sqlGetDateRange():
    id = request.form['id']
    column = request.form['column']
    df = graph[id].df.select(column)
    df = df.withColumn(column, df[column].cast(DateType()))
    dates = df.select(column).where(col(column).isNotNull())
    return jsonify(dates.rdd.min()[0], dates.rdd.max()[0])


@app.route("/sqlJoin", methods=['POST'])
def sqlJoin():
    id_1 = request.form['id_1']
    id_2 = request.form['id_2']
    node_1 = graph[id_1]
    node_2 = graph[id_2]
    return jsonify(node_1.df.schema.names, node_2.df.schema.names)


@app.route('/sqlJoinResponse', methods=['POST'])
def sqlJoinResponse():
    id_1 = request.form['id_1']
    id_2 = request.form['id_2']
    join_columns_1 = eval(request.form['join_columns_1'])
    join_columns_2 = eval(request.form['join_columns_2'])
    join_type = request.form['join_type']

    assert len(join_columns_1) == len(join_columns_2)

    translate_join_type_to_sparksql = {
        "LEFT OUTER JOIN": "left_outer",
        "RIGHT OUTER JOIN": "right_outer",
        "FULL OUTER JOIN": "full_outer",
        "INNER JOIN": "inner",
        "LEFT SEMI JOIN": "left_semi",
        "LEFT ANTI JOIN": "left_anti"}


    node_1 = graph[id_1]
    df_1 = node_1.df

    node_2 = graph[id_2]
    df_2 = node_2.df

    cond = [df_1[join_columns_1[i]] == df_2[join_columns_2[i]] for i in range(len(join_columns_1))]
    df = df_1.join(df_2, on=cond, how=translate_join_type_to_sparksql[join_type])

    node = Node(label=join_type, df=df, inputs=[node_1, node_2])
    graph.add_node(node)

    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}


sql_connections = {}
@app.route('/sqlConnect', methods=['POST'])
def sqlConnect():

    # Get Connection Parameters
    host = request.form['host']
    username = request.form['username']
    password = request.form['password']
    sql_type = request.form['sql_type']

    #example = 'mysql://semjon:visualpassword@db4free.net:3306/visual_prog'
    connect_string = username+":"+ password + "@" + host
    if sql_type =="MySQL":
        connect_string = "mysql://" +connect_string
        sql_engine = sql.create_engine(connect_string)
        df = pd.read_sql_query("show tables", sql_engine)
        tables = df.loc[0].tolist()
        sql_engine.dispose()

    if sql_type =="PostgreSQL":
        connect_string = "postgres://" +connect_string
        sql_engine = sql.create_engine(connect_string)
        df = pd.read_sql_query("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';", sql_engine)
        tables = df["tablename"].tolist()
        sql_engine.dispose()

    connection_id = uuid.uuid1().hex
    sql_connections[connection_id] = connect_string


    return jsonify(tables, connection_id)

@app.route("/addSQLTable", methods=['POST'])
def addSQLTable():
    # Get connection parameters
    connection_id = request.form['connection_id']
    table_name = request.form['table_name']

    # Connect to database
    sql_engine = sql.create_engine(sql_connections[connection_id])
    query = "select * from " + table_name
    pandas_df = pd.read_sql_query(query, sql_engine)
    sql_engine.dispose()
    df = spark.createDataFrame(pandas_df)

    # Create Node and add to graph
    node = Node(label="SQL: "+str(table_name), df=df)
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps([])}

@app.route("/hqlSaveTable", methods=['POST'])
def hqlSaveTable():
    id = request.form['id']
    db = request.form['db']
    name = request.form['name']

    tables = spark.sql("show tables in "+str(db)).toPandas()
    tables = tables["tableName"].tolist()
    if name in tables:
        return

    df = graph[id].df
    df.createOrReplaceTempView("tmp")
    query = "create table {}.{} as select * from tmp".format(db,name)
    print(query)
    spark.sql(query)
    return "success"


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

@app.route('/upload.html',methods = ['POST'])
def uploadCSV():
    # Create variable for uploaded file
    df = pd.read_csv(request.files.get('file'))
    print(df)
    name = request.form['name']
    df = spark.createDataFrame(df)

    # Create Node and add to graph
    node = Node(label="CSV: " + str(name), df=df)
    graph.add_node(node)
    return redirect(url_for("hello"))

######################## Streaming ###############################

@app.route('/addStreamingDataSource', methods=['POST'])
def addStreamingDataSource():
    id = request.form['id']
    node = StreamingSource(id, ssc)
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps([])}


streaming_data = dict()


@app.route('/vizualizeStream', methods=['POST'])
def vizualizeStream():
    id = request.form['id']
    source = graph[id]
    stream = source.stream

    stream.foreachRDD(lambda time, rdd: AverageAndStd(time, rdd, streaming_data, id))

    node = StreamingNode(label="Visualize", stream=stream, inputs=[source])
    graph.add_node(node)
    ssc.start()

    while not id in streaming_data:
        time.sleep(1)

    df = streaming_data[id]
    plots.append(make_line_plot(df_to_dict(df), id))

    # ssc.awaitTermination()
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}


@app.route('/windowedStreamResponse', methods=['POST'])
def windowedStreamResponse():
    # Get selected Node from Graph
    id = request.form['id']
    source = graph[id]
    stream = source.stream
    #
    windowLength = int(request.form['windowLength'])
    windowInterval = int(request.form['windowInterval'])
    if request.form['aggregationFunction'] == "Sum":
        windowed_stream = stream.window(windowLength, windowInterval)
        windowed_stream = windowed_stream.reduce(reduceSum)
    if request.form['aggregationFunction'] == "Mean":
        windowed_stream = stream.map(countStream).window(windowLength, windowInterval).reduce(reduceSum).map(
            count_to_mean)
    windowed_stream.pprint()
    ssc.start()
    node = StreamingNode(label="Window: \n" + str(windowLength) + "s / " + str(windowInterval) + " s", stream=stream,
                         inputs=[source], ssc=ssc)
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}

plots = []
@app.route('/dashboard/')
def show_dashboard():
    global plots
    return render_template('plots.html', plots=plots)


x = 0
@app.route('/data/<id>/', methods=['POST'])
def data(id):
    global streaming_data
    print(streaming_data)

    return jsonify(**df_to_dict(streaming_data[id]))

@app.route('/submitPlot', methods=['POST'])
def submitPlot():
    print("submitplot")
    id = request.form['id']
    print(request.form)
    columns = eval(request.form['columns'])
    print(columns)

    source = graph[id]
    df = source.df.toPandas()

    print(columns)
    print(id)

    node = Node("Vizualize",df=None,inputs=[source])
    plots.append(make_scatter_plot(df, columns))
    graph.add_node(node)
    return {"node": json.dumps(node.get_Cyto_node()), "edges": json.dumps(node.get_Cyto_edges())}

if __name__ == '__main__':
    app.run()
