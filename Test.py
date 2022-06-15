"""
This is a test python script
"""


# Databricks notebook source
print('hello world')
# COMMAND ----------

# DBTITLE 1,Some title here
TEMP_VAR = 2 + 2
print(TEMP_VAR)

# COMMAND ----------

#Another test here

# COMMAND ----------

#asdfasdf

def some_func(input_str: str):
    """
    prints the inputted string
    """
    print(f"this is a func: {input_str}")


if __name__ == '__main__':
    some_func("hello world")
