package com.falkonry.data.utils

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.expressions.*
import org.apache.spark.sql.api.java.*
import java.sql.Timestamp
import java.time.Duration

class GapFiller {
    companion object {
        val dtCol="__DeltaT__"
        val gapThreshold = 100
        val medianDivider = 10

        @JvmStatic fun main(args: Array<String>) {
            val file =
                "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\PI Tag Extractions\\Joined\\OakSpirit_Bypass_1st_Stage_Valve_Position_1.csv"
            val timeColumn = "Timestamp(UTC)"

            Logger.getLogger("org").setLevel(Level.ERROR)
            Logger.getLogger("akka").setLevel(Level.ERROR)

            // Load csv into DataSet
            val sess = SparkSession.builder().appName("Fill Gaps CSV").master("local").orCreate
            var ds = sess.read().format("csv").option("header", "true").load(file) // Really a DataFrame not a Data Set
            val ds2 = getDts(ds, timeColumn)

            // Show rows ds2.show(10)

            // Get expected gap
            val gapTh= getGapThreshold(ds2,timeColumn)
            println("Gap Threshold: ${gapTh}")

            // Get gap rows
            val ds3=getGapsToFill(ds2,gapTh,timeColumn)
            ds3.show(10)
        }

        class DiffUDF {
            companion object Diff : UDF2<Timestamp?, Timestamp?, Long?> {
                override fun call(next: Timestamp?, prev: Timestamp?): Long? {
                    if (next == null || prev == null)
                        return null
                    return Duration.between(prev.toInstant(), next.toInstant()).toNanos()
                }
            }
        }

       fun getDts(ds: Dataset<Row>, timeColumn: String): Dataset<Row> {
            // Register UDF
            ds.sqlContext().udf().register("getDiff", DiffUDF, DataTypes.LongType)

            var tsCol = "__" + timeColumn + "__"
            val df1 = ds.withColumn(tsCol, to_timestamp(ds.col(timeColumn)))

            // Set window for window functions ordering by timestamp
            val byTs = Window
                .orderBy(ScalaUtils.getScalaSeq(arrayOf(df1.col(tsCol))))

            // Get intervals size after sorting by timestamp order
            val df2 = df1.withColumn(
                dtCol,
                callUDF("getDiff", ScalaUtils.getScalaSeq(arrayOf(df1.col(tsCol), lag(df1.col(tsCol), 1).over(byTs))))
            )
            return df2
        }

        // Get expected gap
        fun getGapThreshold(df:Dataset<Row>,timeColumn: String):Long
        {
            var dfToUse=df
            if(!df.columns().contains(dtCol)){
                dfToUse=getDts(df,timeColumn)
            }
            dfToUse.createOrReplaceTempView("df")
            return gapThreshold*dfToUse.sparkSession().sql("select approx_percentile("+dtCol+", 0.5) as median from df").first().getLong(0)/ medianDivider
        }

        // Get gaps to fill
        fun getGapsToFill(df:Dataset<Row>,gapNanos:Long,timeColumn: String):Dataset<Row>{
            var dfToUse=df
            if(!df.columns().contains(dtCol)){
                dfToUse=getDts(df,timeColumn)
            }
            return dfToUse.filter(dtCol+">="+gapNanos)
        }

        // Replace gaps
        fun replaceGaps(df: Dataset<Row>, gapNanos:Long, timeColumn: String)
        {
            var dfToUse=df
            if(!df.columns().contains(dtCol)){
                dfToUse=getDts(df,timeColumn)
            }
            // Get new data set of only the gaps
            var dfGaps=dfToUse.select(ScalaUtils.getScalaSeq(arrayOf(col(dtCol))))




            val byDt = Window
                .partitionBy(ScalaUtils.getScalaSeq(arrayOf(dfGaps.col(dtCol))))
            /*
            dfToUse.getRows().foreach()
            {

            }
             */
        }
    }
}
