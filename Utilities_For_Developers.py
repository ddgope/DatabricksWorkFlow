# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Utilities
# MAGIC 1. File System
# MAGIC 1. Workflow
# MAGIC 1. Widget
# MAGIC 1. Secrets
# MAGIC 1. Library

# COMMAND ----------

help(dbutils.fs)

# COMMAND ----------

dbutils.fs.mkdirs("/newdir")

# COMMAND ----------

dbutils.fs.ls("/newdir")

# COMMAND ----------

dbutils.fs.mkdirs("/newdir/new")

# COMMAND ----------

display(dbutils.fs.ls("/newdir"))

# COMMAND ----------

dbutils.fs.put("/newdir/new.txt","Demo text")

# COMMAND ----------

dbutils.fs.ls("/newdir")

# COMMAND ----------

dbutils.fs.head("/newdir/new.txt")

# COMMAND ----------

dbutils.fs.mv("/newdir/new.txt","/newDir/new")

# COMMAND ----------

display(dbutils.fs.ls("/newdir/new/"))

# COMMAND ----------

dbutils.fs.cp("/newDir/new/new.txt","/newdir")

# COMMAND ----------

dbutils.fs.rm("/newDir/new.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Widgets

# COMMAND ----------

dbutils.widgets.text("api_key","","API KEY")

# COMMAND ----------

dbutils.widgets.get("api_key")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Workflow

# COMMAND ----------

dbutils.notebook.run("Workflow",timeout_seconds=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Library

# COMMAND ----------

import sympy

# COMMAND ----------

dbutils.library.installPyPI("sympy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secrets

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("Azure")

# COMMAND ----------

dbutils.secrets.get("Azure","Api Key")

# COMMAND ----------

