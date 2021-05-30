// Databricks notebook source
spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = true


// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/myadls/

// COMMAND ----------

val list = Seq(
 (1, "A"),
 (2, "B"),
 (3, "C")
)

val empDF = list.toDF("id", "name")

display(empDF)


// COMMAND ----------

empDF.write.format("delta").save("/tmp/deltalake/emp")

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls /tmp/deltalake/emp

// COMMAND ----------

val list1 = Seq(
 (4, "D", "A@A.com"),
 (5, "E", "B@B.com"),
 (6, "F", "C@C.co,")
)

val empDF1 = list1.toDF("id", "name", "email")

empDF1.write.format("delta").mode("append").option("mergeSchema","true").save("/tmp/deltalake/emp/") 

// COMMAND ----------

val df1 = spark.read.format("delta").load("/tmp/deltalake/emp/")

display(df1)

// COMMAND ----------

Map( (1, "A"), (2,"B"))

// COMMAND ----------

// DBTITLE 1,update
import io.delta.tables._
import org.apache.spark.sql.functions._

val empDeltaTable = DeltaTable.forPath(spark, "/tmp/deltalake/emp/")

empDeltaTable.update(
                    condition = expr("id == 2") ,
                    set =  Map("name" -> lit("PRAKASH"), "email" -> lit("prakash.chenga@gmail.com"))
                  )
  
  //UPDATE emp SET name = 'PRAKASH' AND email = 'prakash.chenga@.com' WHERE id =2

// COMMAND ----------

val empDelta = spark.read.format("delta").load("/tmp/deltalake/emp/")

display(empDelta)

// COMMAND ----------

// DBTITLE 1,Delete
empDeltaTable.delete(condition = expr("id == 2"))

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /tmp/deltalake/emp1

// COMMAND ----------

// DBTITLE 1,merge: upsert(update/insert)
//Source data/ source table 
val empSourceDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/myblob/emp_dataset.csv")
//display(empSourceDF)

import io.delta.tables._

//Primary Key : id
val mergeCondition = "tgt.id = src.id"
//Initial load
if(DeltaTable.isDeltaTable("/tmp/deltalake/emp1/")){
     
     println("merge")
      //Target 
       val deltaTable = DeltaTable.forPath(spark, "/tmp/deltalake/emp1/")
       deltaTable.alias("tgt")
          .merge(empSourceDF.alias("src"), mergeCondition)
          .whenMatched().updateAll() // Update  or   LEFT OUTER JOIN   
          .whenNotMatched().insertAll() // insert new records or LEFT ANTI 
          .execute()
              
}else{
   println("initial load")
   empSourceDF.write
              .format("delta")
              .mode("append")
              .save("/tmp/deltalake/emp1/")
}

// COMMAND ----------

val deltOutput = spark.read.format("delta").load("/tmp/deltalake/emp1/")

display(deltOutput)