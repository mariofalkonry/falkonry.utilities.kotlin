package com.falkonry.aptar

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes


fun main(args: Array<String>)
{
    var file="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC Nov 2019.parquet"
    var entityFile="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC vars.csv"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Load entity map
    var entities=loadEntityMap(entityFile)

    // Create session
    val spark= SparkSession.builder().appName("Apter Colmec").master("local").orCreate

    // Define lambda to get entity from as a UDF1<String,String>
    var getEntityName:(String)->String?={fin->entities[fin.toLowerCase().substring(0,fin.lastIndexOf('_'))]}
    spark.udf().register("entityNameUDF",
        UDF1 { columnValue: String -> getEntityName(columnValue) } as UDF1<String, String>,
        DataTypes.StringType
    )

    // Load parquet file using spark
    val dsin: Dataset<Row> = spark.read().parquet(file)
    dsin.printSchema()
    dsin.createOrReplaceTempView("dsin")
    var dates=spark.sql("select min(timestring) as min_date,max(timestring) as maxdate from dsin")
    dates.show()

    // Add entity column populated using UDF
    val dsin_ent= dsin.withColumn(
        "entity",
        callUDF("entityNameUDF", col("filename")))
    dsin_ent.createOrReplaceTempView("dsin")

    // Select into new ds
    val dsout=spark.sql("select entity,varname,timestring,cast(replace(varvalue,',','.') as double) as varvalue " +
            "from dsin " +
            "where varname not like '\$RT_%'")
    dsout.show(10)

    dsout.write().mode(SaveMode.Overwrite).csv(file.replace(".parquet","_parquet"))
}
