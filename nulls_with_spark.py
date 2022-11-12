from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark=SparkSession.builder.master("local")\
      .appName("nulls_with_spark").getOrCreate()

data=[ (1,"suren",29,"developer",40000, 45000),
       (2,"ashok",24,"manager",50000, None),
       (3,"naveen",29, "",None, 33000),
       (4,"sharmi",28,"manager",None, None),
       (None, None, None, None, None, None)
       ]
columns=["id","employee","age","role","salary_2021", "salary_2022"]

df_employees=spark.createDataFrame(data, columns)
df_employees.show()

#count
print("Total dataframe row count ",df_employees.count())
print("Employee column count",df_employees.select("employee").count())

df_employees.select(count(df_employees.employee)).show()

# check empty string and nulls behaviour in groupby aggregations
print("Aggregation on column with nulls below")
df_employees.groupby("role").count().show()

## logical operations

# when you apply logical operations or join operation on a column which is null 
# then the entire row is ignored
print("comparison operations on two columns with nulls below")
df_employees.filter(df_employees.salary_2022 > df_employees.salary_2021).show()

city_data=[
    (1,"chennai"),
    (2,"Trivandrum"),
    (3,"salem"),
    (4,"chennai"),
    (None, None)
 ]
columns_location=["id","city"]
df_location=spark.createDataFrame(city_data,columns_location)
df_location.show()


#null key from both columns are ignored.
df_emp_loc=df_employees.join(df_location,on=["id"], how="inner")
print("Employees table joined with location table below")
df_emp_loc.show()