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

# # Read files from raw_data

# CELL ********************

df_customers=spark.read.format('csv').option('header',True).option('inferSchema',True).load('Files/raw_data/customers.csv')
df_orders=spark.read.format('csv').option('header',True).option('inferSchema',True).load('Files/raw_data/orders.csv')
df_payments=spark.read.format('csv').option('header',True).option('inferSchema',True).load('Files/raw_data/payments.csv')
df_support_tickets=spark.read.format('csv').option('header',True).option('inferSchema',True).load('Files/raw_data/support_tickets.csv')
df_web_activities=spark.read.format('csv').option('header',True).option('inferSchema',True).load('Files/raw_data/web_activities.csv')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### check loading dataframes


# CELL ********************

print('customers :')
display(df_customers.limit(3))
print('orders :')
display(df_orders.limit(3))
print('pyments :')
display(df_payments.limit(3))
print('support_tickets :')
display(df_support_tickets.limit(3))
print('web_activities :')
display(df_web_activities.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Ingest data into Bronze layer


# CELL ********************

df_customers.write.format('delta').mode('overwrite').saveAsTable('LH_ecommerce.Bronze.customers')
print('customers data loaded successfully in Bronze layer:')

df_orders.write.format('delta').mode('overwrite').saveAsTable('LH_ecommerce.Bronze.orders')
print('orders data loaded successfully in Bronze layer:')

df_payments.write.format('delta').mode('overwrite').saveAsTable('LH_ecommerce.Bronze.payments')
print('payments data loaded successfully in Bronze layer:')

df_support_tickets.write.format('delta').mode('overwrite').saveAsTable('LH_ecommerce.Bronze.support_tickets')
print('support_tickets data loaded successfully in Bronze layer:')

df_web_activities.write.format('delta').mode('overwrite').saveAsTable('LH_ecommerce.Bronze.web_activities')
print('web_activities data loaded successfully in Bronze layer:')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Check data bin bronze layer

# CELL ********************

query=spark.sql('select * from Bronze.customers limit 5')
display(query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables=["customers","orders","payments","support_tickets","web_activities "]
for table in tables:
    query=spark.sql(f'select * from Bronze.{table} limit 5')
    print(f'{table} table')
    display(query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
