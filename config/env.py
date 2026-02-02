#config/env.py
import os
def pyspark_env():
    python_path = r"C:\Users\Abhay\AppData\Local\Programs\Python\Python310\python.exe"
    HADOOP_HOME = r"C:\hadoop"

    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_HOME, "bin")



