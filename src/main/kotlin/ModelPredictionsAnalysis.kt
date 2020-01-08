package com.falkonry.utils

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*


fun main(args: Array<String>)
{
    //var file="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC Nov 2019.parquet"
//    var filePredictions="/Users/blackie/Documents/Projects/Falkonry/Aptar/predictions-BiVis1-M11.csv"
//    var fileEvents="/Users/blackie/Documents/Projects/Falkonry/Aptar/events-BiVis1.csv"
//    var filePredictions="/Users/blackie/Documents/Projects/Falkonry/Aptar/predictions-ExtTunnel-M24.csv"
//    var fileEvents="/Users/blackie/Documents/Projects/Falkonry/Aptar/events-ExtTunnel.csv"

    var pred="pre"
    var startTime="2019-07-31T00:00:00.000000Z"
    var endTime="2019-08-14T00:00:00.000000Z"
    var filePredictions="/Users/blackie/Documents/Projects/Falkonry/Molex/predictions.csv"
    var fileEvents="/Users/blackie/Documents/Projects/Falkonry/Molex/events.csv"
    var fileOut="/Users/blackie/Documents/Projects/Falkonry/Molex/precursors.csv"
    var fileStats="/Users/blackie/Documents/Projects/Falkonry/Molex/stats.csv"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create spark session
    val spark= SparkSession.builder().appName("Falkonry").master("local").orCreate

    // Load events csv file using spark
    var events: Dataset<Row> = spark.read().option("header","true").csv(fileEvents)

    // Add rownumber
    var windowSpec = Window.partitionBy("entity").orderBy("time")
    events = events.withColumn("rownum", row_number().over(windowSpec))

    // Select new view that has end of previous event (e0) and start of next on (e1)
    events.createOrReplaceTempView("events")
    var events_view=spark.sql("select e1.entity,e2.value,e1.end as e0,e2.time as e1 from events e1 join events e2 on e1.entity=e2.entity and e1.rownum=e2.rownum-1")
    events_view.createOrReplaceTempView("events_view")
    events_view.show(10)


    // Load predictions csv file using spark
    var preds: Dataset<Row> = spark.read().option("header","true").csv(filePredictions)
    preds.createOrReplaceTempView("preds")

    // Filter to only predictions of interest
    var pred_interest=preds.filter("value='"+pred+"'")

    // Select view that finds all predictions that fall between e0 and e1 full outerjoin
    pred_interest.createOrReplaceTempView("preds")
    var preds_view=spark.sql("select upper(coalesce(p.entity,e.entity)) as entity,p.time as pred_time,e.e1 as event_time,(to_unix_timestamp(timestamp(e.e1))-to_unix_timestamp(timestamp(p.time)))/60 as minutes_before,p.value as prediction,e.value as event from preds p full join events_view e on lower(p.entity)=lower(e.entity) and p.time>e.e0 and p.time<=e.e1")
    preds_view.show(50)

    // Save to single csv file
    preds_view.repartition(1).write().option("header","true").mode(SaveMode.Overwrite).csv(fileOut)

    // Summarize them (max,min,avg)
    preds_view.createOrReplaceTempView("preds_view")
    var stats_view=spark.sql("select entity,prediction,event,event_time,max(minutes_before),min(minutes_before),avg(minutes_before) from preds_view group by entity,prediction,event,event_time order by event_time")
    stats_view.show(50,false)

    // Save to csv file
    stats_view.repartition(1).write().option("header","true").mode(SaveMode.Overwrite).csv(fileStats)

    // Compute F1 Score
    stats_view.createOrReplaceTempView("stats")

    print("done")
}
