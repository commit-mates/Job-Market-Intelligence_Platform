# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/database_files/csv_files/customers-100.csv",header=True,inferSchema = True)
display(df)
