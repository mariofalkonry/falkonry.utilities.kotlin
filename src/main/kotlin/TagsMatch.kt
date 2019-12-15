import com.falkonry.data.utils.ScalaUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.api.java.function.ForeachFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import java.io.File

fun closest(s:String?, m:Array<String?>, caseInsensitive:Boolean=false):Pair<String,Int>
{
    var ret= Pair("",Int.MAX_VALUE)
    m.forEach()
    {
        var toMatch=it
        s?.let{
            toMatch?.let {
                var dist=levenshtein(if(caseInsensitive) {s.toLowerCase()} else {s} ,if(caseInsensitive) {toMatch.toLowerCase()} else {toMatch})
                if(dist<ret.second)
                    ret=Pair(toMatch,dist)
            }
        }
    }
    return ret
}

fun main(args: Array<String>) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val file = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\TagsForPI\\Yamal and Oak Tags.csv"
    val sourceColumns=arrayOf("OAK Tag","OAK Description")
    val matchColumns=arrayOf("Yamal Tag","Yamal Description")


    // Load using Spark SQL
    val sess= SparkSession.builder().appName("Tags Match").master("local").orCreate

    // Load csv
    var indata= sess.read().format("csv").option("header","true").load(file)

    // Save to file
    var fout=file.replace(".","_matches1.")

    // Do one direction
    matchStringsIntoFile(fout,indata,sourceColumns, matchColumns)

    // Reverse it
    fout=file.replace(".","_matches2.")
    matchStringsIntoFile(fout,indata,matchColumns,sourceColumns)
}


fun matchStringsIntoFile(fout:String,indata:Dataset<Row>,sourceColumns:Array<String>,matchColumns:Array<String>)
{
    // Prepare separate data sets
    val sourcedata=indata.select(ScalaUtils.getScalaColumnSeq(sourceColumns))
    var matchdata=indata.select(ScalaUtils.getScalaColumnSeq(matchColumns))
    var matchboth=indata.withColumn("both",concat(ScalaUtils.getScalaSeq(arrayOf(indata.col(matchColumns[0]),
        lit(" - "),indata.col(matchColumns[1])))))
    var bothValues=matchboth.select(ScalaUtils.getScalaColumnSeq(arrayOf("both"))).toJavaRDD().map{r->r.getString(0)}.collect().toTypedArray()


    // Prepare outputs
    var matchTags=mutableListOf<Pair<String,Int>>()
    var matchDescrs= mutableListOf<Pair<String,Int>>()
    var matchTagDescrs= mutableListOf<Pair<String,Int>>()
    var matchDataTagList=matchdata.select(ScalaUtils.getScalaColumnSeq(arrayOf(matchColumns[0]))).toJavaRDD().map{r->r.getString(0)}.collect().toTypedArray()
    var matchDataDescrList=matchdata.select(ScalaUtils.getScalaColumnSeq(arrayOf(matchColumns[1]))).toJavaRDD().map{r->r.getString(0)}.collect().toTypedArray()
    var matchDataBothList=matchboth.select(ScalaUtils.getScalaColumnSeq(arrayOf("both"))).toJavaRDD().map{r->r.getString(0)}.collect().toTypedArray()
    // For each function to be called to find matches
    var foreachFunction:ForeachFunction<Row> = ForeachFunction {
        var tag=it.getString(it.fieldIndex(sourceColumns[0]))
        var descr=it.getString(it.fieldIndex(sourceColumns[1]))
        var matchTag=Pair("",Int.MAX_VALUE)
        tag.let {
            matchTag=closest(tag,
                matchDataTagList
                ,true)
        }
        matchTags.add(matchTag)
        var matchDescr=Pair("",Int.MAX_VALUE)
        descr.let {
            matchDescr=closest(descr,
                matchDataDescrList
                ,true)
    }
        matchDescrs.add(matchDescr)
        var tagdescr=tag+" - "+descr
        var both=Pair("",Int.MAX_VALUE)
        tagdescr.let{
            both=closest(tagdescr,
                matchDataBothList
                ,true)
        }
        matchTagDescrs.add(both)
    }

    // Execute the foreach loop
    sourcedata.foreach(foreachFunction)

    data class Match(val `Match Tag`:String,val `Tag Distance`:Int,
                     val `Match Description`:String,val `Descr Distance`:Int,
                     val `Match Both`:String,val `Both Distance`:Int)

    var matches:MutableList<Match> = mutableListOf()

    for((i,t) in matchTags.withIndex())
    {
        var d=matchDescrs[i]
        var b=matchTagDescrs[i]
        var match=Match(t.first,t.second,d.first,d.second,b.first,b.second)
        matches.add(match)
    }

    var outdata=sourcedata.sparkSession().createDataFrame(matches,Match::class.java)

    // Delete old file if exist
    if(File(fout).exists())
        File(fout).delete()
    // Save to file
    outdata.write().mode(SaveMode.Overwrite).format("csv").option("header","true").save(fout);
}