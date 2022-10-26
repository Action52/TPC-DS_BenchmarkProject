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


* * *
### Running the pipeline

To run the pipeline, let's first create a job with databricks. 
First, edit the scripts/run_tpcds_notebook_on_databricks.json file to contain your mail:

```
  "email_notifications": {
    "on_success": [
      "<YOUR-MAIL-HERE>"
    ],
    "on_failure": [
      "<YOUR-MAIL-HERE>"
    ]
  }
```

```
databricks jobs configure --version=2.1     
databricks jobs create --json-file scripts/run_tpcds_notebook_on_databricks_job.json
```

This json file will create a databricks job that will, when triggered, execute the jupyter notebook
with the tpcds pipeline that we have generated. You can also check the notebooks manually at the databricks web UI.  
Upon creation, the CLI will return a "job-id" that we can use to call the job.
You can always check the job-id of your created jobs by doing:

```
databricks jobs list 
```

This json file will create a databricks job that will, when triggered, execute the jupyter notebook
with the tpcds pipeline that we have generated. You can also check the notebooks manually at the databricks web UI.

To trigger the job, run this command:

```
databricks jobs list 
```