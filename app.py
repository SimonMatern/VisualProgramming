from flask import Flask, request, jsonify
from flask import  render_template
import json
import os
# -------- Spark imports  --------
import pyspark
import os
from os.path import expanduser, join, abspath
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
# -------- Spark imports  --------
#os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop"
os.environ["SPARK_HOME"]="/home/nodeuser/nfs_share/spark-2.4.4-bin-hadoop2.7"
warehouse_location = abspath('/user/hive/warehouse')
#sc = SparkContext(master = "yarn-client")

"""
sconf = SparkConf().setAll([('master','yarn'),('deploy-mode','client'),('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'),
                            ('spark.driver.memory','4g'),('spark.executor.instances', 10), ('spark.sql.warehouse.dir', warehouse_location)])
spark = SparkSession \
    .builder \
    .config(conf=sconf) \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark-Session Created!")
data_sources = spark.sql("show tables in default").toPandas()
data_sources = data_sources["tableName"].tolist()
print(data_sources)
"""

app = Flask(__name__)

data = ["data1", "data2", "data3", "data4"]


@app.route('/')
def hello_world():
    global data_sources
    return render_template("ui.html", data=data)

@app.route('/get_node_ui', methods=['GET', 'POST'])
def get_node_ui():
    if request.method == 'POST':
        jsdata = request.form['data']
        print(jsdata)
    return "RPC-Flask"

if __name__ == '__main__':
    app.run()
