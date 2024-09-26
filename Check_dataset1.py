# Databricks notebook source
count_false = spark.read.table("results_transformed_data").filter("is_valid = False").count()
if count_false > 0:
    raise Exception("Found False in is_valid column")
