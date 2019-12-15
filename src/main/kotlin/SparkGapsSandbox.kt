import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.*
import kotlin.random.Random
import java.io.Serializable

// Data class by itself did not work.  Missing parametersless constructor
class Gap:Serializable
{
    var i:Int=0
    var g:Double=0.0
    constructor()  // Required to deserialize
    constructor(i:Int,g:Double) // Required for convenience in sequence generation
    {
        this.i=i
        this.g=g
    }
}

fun main(args: Array<String>) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a sequence of objects
    val maxGap=7.4
    var i=0
    var gaps=MutableList(40){
        Gap(i++,maxGap * Random.nextDouble())
    }

    // Create Encoder (aka serializer)
    var encoder=Encoders.bean(Gap::class.java)

    // Create a dataset from it
    val spark=SparkSession.builder().appName("Gaps-R-Us").master("local").orCreate
    var ds=spark.createDataset(gaps,encoder)
//    ds.printSchema()
//    ds.show(40)  // Weird values if using data class in Kotlin???
//    var objGap=ds.first()
//    println("g=${objGap.g},i=${objGap.i}")

    // Select gaps that are between two limits
    val loLim=maxGap*0.2
    val hiLim=maxGap*0.8

    var gapsToFill=ds.filter{it.g>=loLim && it.g<=hiLim}
    gapsToFill.show(40)
}