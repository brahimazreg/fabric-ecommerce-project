# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c92bd9fe-e0ff-441b-b834-b83254b376d0",
# META       "default_lakehouse_name": "LH_ecommerce",
# META       "default_lakehouse_workspace_id": "40d7c0d2-acc5-4ec8-aa2a-dd9619840388",
# META       "known_lakehouses": [
# META         {
# META           "id": "c92bd9fe-e0ff-441b-b834-b83254b376d0"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Read data from bronze layer
# 
#  


# CELL ********************

customers =spark.sql(f'select * from Bronze.customers ')
orders =spark.sql(f'select * from Bronze.orders ')
payments =spark.sql(f'select * from Bronze.payments ')
support_tickets =spark.sql(f'select * from Bronze.support_tickets  ')
web_activities =spark.sql(f'select * from Bronze.web_activities')
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### check data 


# CELL ********************

dfs = [customers, orders, payments, support_tickets, web_activities]
for df in dfs:
    print(f'data frame')
    display(df.limit(3))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Clean customers dataframe

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

customers_cleaned=(
    customers.withColumn("email",lower(trim(col('email'))))
             .withColumn("name", initcap(trim(col('name'))))
             .withColumn("gender",when(lower(trim(col("gender"))).isin('f','FEMALE','female') ,"Female")
                                  .when(lower(trim(col("gender"))).isin('m','MALE') ,"Male")
                                  .otherwise("other"))
            .withColumn("dob",to_date(regexp_replace(col('dob'),'/','-'))  )
            .withColumn("location",initcap(trim(col("location"))) )
            .dropDuplicates(["customer_id"])    
            .dropna(subset=["customer_id","email"])                               
               
             
)
customers_cleaned.write.format("delta").mode("overwrite").saveAsTable("LH_ecommerce.Silver.customers")
print("customers data successfully saved in silver layer")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 

# MARKDOWN ********************

# 


# MARKDOWN ********************

# ##### Clean orders dataframe

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

orders_cleaned = (
    orders
    .withColumn("order_date", 
                when(col("order_date").rlike("^\d{4}/\d{2}/\d{2}$"), to_date(col("order_date"), "yyyy/MM/dd"))
                .when(col("order_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("order_date"), "dd-MM-yyyy"))
                .when(col("order_date").rlike("^\d{8}$"), to_date(col("order_date"), "yyyyMMdd"))
                .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"])
)
orders_cleaned.write.format("delta").mode("overwrite").saveAsTable("Silver.orders")
print("orders data successfully saved in silver layer")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 


# MARKDOWN ********************

# ##### Clean payment dataframe

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

mapping = {
    "creditcard": "Credit Card",
    "credit card": "Credit Card",
    "cc": "Credit Card"
}
payments_cleaned = (
    payments
    .withColumn("payment_date", to_date(regexp_replace(col("payment_date"), "/", "-")))
    .withColumn("payment_method", initcap(col("payment_method")))
    .replace(mapping, subset=["payment_method"])    
    .withColumn("payment_status", initcap(col("payment_status")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .dropna(subset=["customer_id", "payment_date", "amount"])
)
payments_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.payments")
print("payments data successfully saved in silver layer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Clean support dataframe

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
mapping = {
    "NA": "None",
    "": "None",
    " ": "None"
}

support_cleaned = (
    support_tickets
    .withColumn("ticket_date", to_date(regexp_replace(col("ticket_date"), "/", "-")))
    .withColumn("issue_type", initcap(trim(col("issue_type"))))
    .withColumn("resolution_status", initcap(trim(col("resolution_status"))))
    .replace(mapping , subset=["issue_type", "resolution_status"])
    .dropDuplicates(["ticket_id"])
    .dropna(subset=["customer_id", "ticket_date"])
)
support_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.support")
print("support_tickets data successfully saved in silver layer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Clean wen dataframe

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType

web_cleaned = (
    web_activities
    .withColumn("session_time", to_date(regexp_replace(col("session_time"), "/", "-")))
    .withColumn("page_viewed", lower(col("page_viewed")))
    .withColumn("device_type", initcap(col("device_type")))
    .dropDuplicates(["session_id"])
    .dropna(subset=["customer_id", "session_time", "page_viewed"])
)
web_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.web")
print("web_activities data successfully saved in silver layer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### create customer360 in gole layer

# CELL ********************

cust = spark.table("silver.customers").alias("c")
orders = spark.table("silver.orders").alias("o")
payments = spark.table("silver.payments").alias("p")
support = spark.table("silver.support").alias("s")
web = spark.table("silver.web").alias("w")

customer360 = (
    cust
    .join(orders, "customer_id", "left")
    .join(payments, "customer_id", "left")
    .join(support, "customer_id", "left")
    .join(web, "customer_id", "left")
    .select(
        col("c.customer_id"),
        col("c.name"),
        col("c.email"),
        col("c.gender"),
        col("c.dob"),
        col("c.location"),

        col("o.order_id"),
        col("o.order_date"),
        col("o.amount").alias("order_amount"),
        col("o.status"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount").alias("payment_amount"),

        col("s.ticket_id"),
        col("s.issue_type"),
        col("s.ticket_date"),
        col("s.resolution_status"),

        col("w.page_viewed"),
        col("w.device_type"),
        col("w.session_time")
    )
)


customer360.write.format("delta").mode("overwrite").saveAsTable("gold.customer360")
print('customer 360 data succssfully inserted in gold layer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from LH_ecommerce.Gold.customer360 limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
