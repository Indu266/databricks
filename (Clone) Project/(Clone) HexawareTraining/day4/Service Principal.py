# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('hexawaresecret')

# COMMAND ----------

dbutils.secrets.get(scope='hexawaresecret',key='clientid')

# COMMAND ----------

container_name       = "bronze"
storage_account_name = "inadlshexaware"
client_id            = "dbutils.secrets.get(scope='hexawaresecret',key='clientid')"
tenant_id            = "dbutils.secrets.get(scope='hexawaresecret',key='tenantid')"
client_secret        = "dbutils.secrets.get(scope='hexawaresecret',key='secretvalue')"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------


