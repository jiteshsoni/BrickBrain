# Databricks notebook source
# MAGIC %pip install -qqqq backoff langgraph-checkpoint-postgres databricks-langchain langgraph==0.5.3 databricks-agents pydantic psycopg[binary] databricks-sdk databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

lakebase_instance = dbutils.widgets.get("lakebase_instance")
app_name = dbutils.widgets.get("app_name")
ka_endpoint_name = dbutils.widgets.get("ka_endpoint_name")

assert lakebase_instance != "", "lakebase_instance notebook parameter must be specified"
assert app_name != "", "app_name notebook parameter must be specified"
assert ka_endpoint_name != "", "ka_endpoint_name notebook parameter must be specified"

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceSecret, AppResourceSecretSecretPermission, AppResourceDatabase, AppResourceDatabaseDatabasePermission, AppResourceServingEndpoint, AppResourceServingEndpointServingEndpointPermission, AppDeployment



# COMMAND ----------

w = WorkspaceClient()

serving_endpoint = AppResourceServingEndpoint(name=ka_endpoint_name,
                                              permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY
                                              )
lakebase_store = AppResourceDatabase(instance_name=lakebase_instance, database_name="databricks_postgres", permission=AppResourceDatabaseDatabasePermission.CAN_CONNECT_AND_CREATE)                                     

secret_app_token = AppResourceSecret(scope="brickbrain-scope", key="slack-app-token", permission=AppResourceSecretSecretPermission.CAN_READ)
secret_bot_token = AppResourceSecret(scope="brickbrain-scope", key="slack-bot-token", permission=AppResourceSecretSecretPermission.CAN_READ)

app_resource = AppResource(name="brickbrain_slackbot_resource", 
                           serving_endpoint=serving_endpoint, 
                           database=lakebase_store, 
                           secret=secret_app_token, 
                           secret=secret_bot_token
                           ) 

agent_app = App(name=app_name, 
              description="Your Databricks assistant", 
              default_source_code_path=os.getcwd(),
              resources=[app_resource])
    
try:
  app_details = w.apps.create_and_wait(app=agent_app)
  print(app_details)
except Exception as e:
  if "already exists" in str(e):
    app_details = w.apps.get(app_name)
    print(app_details)
  else:
    raise e

# COMMAND ----------

deployment = AppDeployment(
  source_code_path=os.getcwd()
)

app_details = w.apps.deploy_and_wait(app_name=app_name, app_deployment=deployment)
print(app_details)