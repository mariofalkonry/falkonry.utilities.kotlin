
import com.falkonry.data.utils.ScalaUtils
import com.google.common.collect.ImmutableMap
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import scala.collection.Seq

fun main(args: Array<String>) {
    val file = "C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\Batches\\AutoclaveBatches_1.csv"
    val signalColumn="signal"
    val valColumn="value"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sess= SparkSession.builder().appName("Pivot CSV").master("local").orCreate

    // Load csv
    var ds= sess.read().format("csv").option("header","true").load(file)
    // Scala Seq
    val keyColumns: Seq<Column> = ScalaUtils.getScalaColumnSeq(ds.columns().filter { it.compareTo(signalColumn)!=0 && it.compareTo(valColumn)!=0 }.toTypedArray())
    ds.show()
    ds=ds.groupBy(keyColumns).pivot(signalColumn).agg(ImmutableMap.of(valColumn,"first"))
    ds.show()

    // Save to csv
    val fout=file.replace(".csv","_pivot")
    ds.coalesce(1).write().format("csv").option("header","true").save(fout)
}