# Databricks Cluster Configuration (Example)

*Cluster name:* spotify-etl-cluster

*Cluster mode:* Single Node (or Standard small cluster)  
*Databricks Runtime:* 12.x (includes Apache Spark 3.x)  
*Node type:* Small (e.g., Standard_D3_v2 on Azure / m5.xlarge on AWS)  
*Auto terminate:* After 60 minutes of inactivity

*Libraries:*
- net.snowflake:snowflake-jdbc:<version>
- net.snowflake:spark-snowflake_<scala_version>:<version>

*Notebook attached:*
- databricks/spotify_notebook.py (imported into Databricks workspace)

*Job schedule:*
- Job name: spotify_etl_databricks_job
- Schedule: Every day at 02:00 AM (or aligned with Snowflake refresh)
- Task: Run the spotify_notebook notebook