import java.io.File
import java.lang.NumberFormatException


class TsStats(count:Int,first:String,last:String) {
    var count:Int=count
    var first:String=first
    var last:String=last
}

fun main(args: Array<String>) {
    //TODO: handle batch case
    val isBatch=false
    val file="Compressor+Monitoring.csv"
    val shiftDays=100
    val rootDir = "C:\\Users\\m2bre\\Documents\\Falkonry\\Demos"
    var entitiesToAdd=listOf("Comp-B","Comp-C")

    // Load extracted data stream into map
    // Entity -> UnixMicros -> Row
    var entityTsCounts=mutableMapOf<String,TsStats>()

    // Read file populating counting timestamps per entity data
    var row=1
    File("${rootDir}\\${file}").bufferedReader().use {
        // Skip header
        // time,signal,entity,value
        it.readLine();
        row++
        var line:String?=null
        while ({ line = it.readLine(); line }() != null) {
            val data=line!!.split(",")
            if(data.size<3) {
                println("Skipping row - less than 3 columns in row $row")
                continue
            }
            val ts=data[0].removePrefix("\"").removeSuffix("\"")
            val entity=data[2].removePrefix("\"").removeSuffix("\"")
            if (entityTsCounts.containsKey(entity))
            {
                var entityStats=entityTsCounts[entity]
                entityStats!!.count++
                if(entityStats.last!=ts)
                    entityStats.last=ts
            } else {
                entityTsCounts.put(entity,TsStats(1,ts,ts))
            }
            row++
        }
    }

    // Find the entity with most values
    var entityToUse=entityTsCounts.maxBy { it.value.count }!!.key
    var maxTs:Long
    try {
        maxTs=entityTsCounts[entityToUse]!!.last.toLong()
    }
    catch(nfe:NumberFormatException)
    {
        println("Unable to convert max ts ${entityTsCounts[entityToUse]!!.last} to long for entity $entityToUse")
        return
    }

    // Write files for each entity with signals shifted
    var idx=1L // Use to compute shift by entity
    var startTs:Long=0
    entitiesToAdd.forEach {
        var entity = it
        val shift=idx++*shiftDays*24*60*60*(10*10*10*10*10*10)
        var f = File("${rootDir}\\Output\\signals_${entity}.csv")
        f.printWriter().use {
            val fout = it
            //TODO: Handle batch case
            fout.println("time,signal,entity,value")
            row = 1
            File("${rootDir}\\${file}").bufferedReader().use {
                val fin = it
                // Skip header
                // time,signal,entity,value
                fin.readLine();
                row++
                var line: String? = null
                while ({ line = it.readLine(); line }() != null) {
                    val data = line!!.split(",")
                    //TODO: Handle batch case
                    if (data.size < 4) {
                        println("Skipping row - less than 4 columns in row $row")
                        continue
                    }
                    val ts = data[0].removePrefix("\"").removeSuffix("\"")
                    //TODO: Handle batch case
                    // val batch=data[1]
                    val entityInSourceFile = data[2].removePrefix("\"").removeSuffix("\"")
                    val signal=data[1]
                    val value=data[3]
                    if (entityInSourceFile == entityToUse) {
                        // Compute new time shift
                        var unixms:Long
                        try {
                            unixms=ts.toLong()
                            if(startTs<1)
                                startTs=unixms
                            unixms+=shift
                            if(unixms> maxTs)
                                unixms=unixms-maxTs+startTs
//                            fout.println("\"$unixms\",$batch,\"$entity\",$signal,$value")
                            fout.println("\"$unixms\",$signal,\"$entity\",$value")
                        }
                        catch(nfe:NumberFormatException)
                        {
                            println("Skipping row - unable to convert $ts to long in row $row")
                        }
                    }
                    row++
                }
            }
        }
    }
}
