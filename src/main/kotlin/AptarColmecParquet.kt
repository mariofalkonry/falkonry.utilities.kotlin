package com.falkonry.aptar

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.*
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes

fun main(args: Array<String>)
{
    //var file="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC Nov 2019.parquet"
    var file="/Users/blackie/Documents/Projects/Falkonry/Aptar/November2019/COLMEC Nov 2019.parquet"
    //var entityFile="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC vars.csv"
    var entityFile="/Users/blackie/Documents/code/java/falkonry.utilities.kotlin/src/main/resources/COLMECvars.csv"
    val timeZone="CET"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Load entity map
    var entities=loadEntityMap(entityFile)

    // Create session
    val spark= SparkSession.builder().appName("Apter Colmec").master("local").orCreate
//    spark.conf().set("spark.sql.session.timeZone", timeZone)

    // Define lambda to get entity from as a UDF1<String,String>
    var getEntityName:(String)->String?={fin->entities[fin.toLowerCase().substring(0,fin.lastIndexOf('_'))]}
    spark.udf().register("entityNameUDF",
        UDF1 { columnValue: String -> getEntityName(columnValue) } as UDF1<String, String>,
        DataTypes.StringType
    )

    // Define lambada to rename columns to valid chars - could be better and more general
    var renameColumn:(String)->String?={sin->sin.replace('(','_').replace(')','_').replace(' ','_')}

    // Load parquet file using spark
    var dsin: Dataset<Row> = spark.read().parquet(file)
    dsin.printSchema()
    dsin.createOrReplaceTempView("dsin")
    var dates=spark.sql("select min(timestring_tz) as min_date,max(timestring_tz) as max_date from dsin")
    dates.show()

    // Add entity column populated using UDF
    val dsin_ent= dsin
//            .toDF(*dsin.columns().map{renameColumn(it)}.toTypedArray())  // Rename attributes
            .withColumn("entity", callUDF("entityNameUDF", col("filename")))
    dsin_ent.printSchema()
    /* Data Class
    data class TS(val Entity:String,val VarName:String,val TimeString:String,val VarValue:String)
    var listOfTS = dsin_renamed.select(col("entity"),
            col("VarName"),
            col("VarValue"),
            col("TimeString(modified time zone)").alias("timezone_tz")).
                collectAsList().
                map({r:Row -> TS(r.getString(0),r.getString(1),r.getString(2),r.getString(3))})
    // Crate new Dataset
    val newDs=spark.createDataFrame(listOfTS,::TS.javaClass).toDF()
    newDs.printSchema()
    newDs.createOrReplaceTempView("dsin")
    spark.sql("select entity,varname,timestring,varvalue,timestring_modified_timezone_ from dsin").show()
     */
    dsin_ent.show()
//    var newFile=file.replace(".parquet","fixed_csv")
//    dsin_ent.write().option("header","true").mode(SaveMode.Overwrite).csv(newFile)

    dsin_ent.createOrReplaceTempView("dsin_ent")

    // Select into new ds
    val dsout=spark.sql("select entity as Entity,varname as VarName,timestring_tz as TimeString,cast(replace(varvalue,',','.') as double) as VarValue " +
            "from dsin_ent " +
            "where varname not like '\$RT_%' and timestring>'2019-10-29 00:00:00'")  // Only want November
    dsout.createOrReplaceTempView("dsout")
    dsout.printSchema()
// bug!! tries to access renamed column    dsout.show(10)

    dates=spark.sql("select min(timestring) as min_date,max(timestring) as max_date from dsout")
    dates.show()

    // Group by entities for sending to different streams
    entities.values.toSet().forEach{
        var entity=it.toLowerCase()
        var dsentity=spark.sql("select * from dsout where lower(entity)='"+entity+"'")
        dsentity.write().mode(SaveMode.Overwrite).option("header","true").csv(file.replace(".parquet","_parquet_"+
                entity.replace('/', '_')))
    }
}
