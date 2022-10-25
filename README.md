# TPC-DS: Spark SQL Implementation using Databricks AWS
* * *
### Requirements and installation

To use this repo we assume you have at least Python3.6 installed on your computer.
Please install the databricks client on a clean python env in your computer.

```
pip install databricks-cli
```

Afterwards, input the access credentials by doing

```
databricks configure
```

This will create a .cfg file on your root folder containing your access data. Please input the data to configure,
the access credentials to configure a test user are included in the written report of this project (Anexes section). 
You can also create your own databricks workspace with your account and later on attach the script and notebook to 
run the pipeline.

Now we'll have to create a spark cluster that will be in charge of processing all the data assignments we send to it.
Jupyter notebooks (such as the one included here) can be attached to databricks. Another option is running a script.
To bring up the cluster, we'll keep using the CLI.

```
databricks clusters create --json-file scripts/create-cluster.json
```

Check the status of the cluster until it is up with 

```
databricks clusters get --cluster-name <CLUSTER-NAME>
```

for the example, the cluster-name is basic-starter-cluster-cli.

* * *
### Running the pipeline

To run the pipeline, let's first create a job with databricks. 
First, edit the scripts/run_tpcds_notebook_on_databricks.json file to contain your mail:

```
{
  "name": "tpcds-job",
  "notebook_task": {
    "source": "GIT",
    "notebook_path": "tpcds-spark.ipynb"
  },
  "email_notifications": {
    "on_success": [
      "<YOUR-MAIL-HERE>"
    ],
    "on_failure": [
      ""<YOUR-MAIL-HERE>"
    ]
  },
  "git_source": {
    "git_url": "https://github.com/Action52/TDC-DS_BenchmarkProject",
    "git_provider": "gitHub",
    "git-branch": "databricks-luis"
  }
}
```

```
databricks jobs configure --version=2.1     
databricks jobs create --json-file scripts/run_tpcds_notebook_on_databricks_job.json
```

This json file will create a databricks job that will, when triggered, execute the jupyter notebook
with the tpcds pipeline that we have generated. You can also check the notebook manually at the databricks web UI.

To execute 