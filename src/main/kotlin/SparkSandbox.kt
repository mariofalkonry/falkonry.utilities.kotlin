import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters
import scala.collection.Seq
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode

fun main(args: Array<String>) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val file = "C:\\Users\\m2bre\\Documents\\Projects\\Molex\\Molding\\SmallFiles\\309682127_Shreyas_18.csv"
    // Scala Seq
    val keyColumns: Seq<String> = JavaConverters.asScalaIteratorConverter(arrayOf("time","signal","entity","batch").iterator()).asScala().toSeq()

    val sess=SparkSession.builder().appName("Spark SQL Sandbox").master("local").orCreate

    // Load csv
    var df= sess.read().format("csv").option("header","true").load(file)
    println("before dups removed=${df.count()}")
    df=df.dropDuplicates(keyColumns)
    println("after dups removed=${df.count()}")

    // Save to csv
    val fout=file.replace(".csv","_nodups")
    df.coalesce(1).write().mode(SaveMode.Overwrite).format("csv").option("header","true").save(fout)
}