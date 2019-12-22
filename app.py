from flask import Flask, request
from flask import  render_template

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
os.environ["HADOOP_CONF_DIR"] = "/usr/local/hadoop/etc/hadoop"

warehouse_location = abspath('/user/hive/warehouse')
#sc = SparkContext(master = "yarn-client")

"""
sconf = SparkConf().setAll([('master','yarn-client'),('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g'),('spark.executor.instances', 10), ('spark.sql.warehouse.dir', warehouse_location)])
spark = SparkSession \
    .builder \
    .master("yarn-client") \
    .config(conf=sconf) \
    .enableHiveSupport() \
    .getOrCreate()
"""

app = Flask(__name__)


@app.route('/')
def hello_world():
    return render_template("ui.html")

@app.route('/get_node_ui', methods=['GET', 'POST'])
def get_node_ui():
    if request.method == 'POST':
        jsdata = request.form['data']
        print(jsdata)
    return "RPC-Flask"

if __name__ == '__main__':
    app.run()
