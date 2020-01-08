package com.falkonry.aptar

import com.falkonry.data.utils.CSVParser
import java.io.File
import java.time.Instant

fun String?.firstLetterUpperCase():String?=this?.substring(0,1)?.toUpperCase()+this?.substring(1)?.toLowerCase()

fun String?.escapeForwardSlash():String? {
    var parts = this?.split('/')
    return parts?.joinToString("\\/")
}

fun loadEntityMap(entityFile:String):MutableMap<String,String>
{
    var entityMap= mutableMapOf<String,String>()  // FileName, PLC aka entity
    var parser= CSVParser(File(entityFile))
    parser.useDataRows(){
            row,ex->
        if(row!=null)
        {
            var file=(parser.getValueByColName("Fichier",row).value as String).toLowerCase()
            var plc=(parser.getValueByColName("PLC",row).value as String).firstLetterUpperCase()
            entityMap.put(file,plc!!)
        }
        else if(ex!=null)
            println("Exception while loading Entity Map: ${ex.message}")
    }
    return entityMap
}

data class SignalStats(val file:String, val start: Instant, val end: Instant, val count:Int)

var stats=mutableMapOf<String,MutableMap<String,MutableMap<Instant,SignalStats>>>()

// Temporary tracking
fun updateSignalStats(stats:MutableMap<String,SignalStats>,signal:String,timeStamp: Instant)
{
    var sigStats:SignalStats?=stats[signal]
    if(sigStats==null)
    {
        var newStats=SignalStats("",timeStamp,timeStamp,1)
        stats.put(signal,newStats)
    }
    else
    {
        // Update stats
        stats[signal]=SignalStats(sigStats.file,sigStats.start,timeStamp,sigStats.count+1)
    }
}

fun saveStats(entity:String, file:String, sigStats:MutableMap<String,SignalStats>){
    sigStats.forEach(){(signal,sigs)->
        val start = sigs.start
        val timeStats = SignalStats(file, sigs.start, sigs.end, sigs.count)  // Replace file name
        var entityStats = stats.getOrDefault(entity, mutableMapOf<String, MutableMap<Instant, SignalStats>>())
        // Don't have that entity
        if (entityStats.isEmpty()) {
            entityStats.put(signal, mutableMapOf(Pair(start, timeStats)))
            stats.put(entity, entityStats)
        } else {
            var signalStats = entityStats.getOrDefault(signal, mutableMapOf<Instant, SignalStats>())
            // Don't have that signal
            if (signalStats.isEmpty()) {
                signalStats.put(start, timeStats)
                entityStats.put(signal, signalStats)
            } else {
                // Check it crashing start time
                var exist: SignalStats? = signalStats[start]
                if (exist != null)
                    throw(UnsupportedOperationException("Entity $entity, signal $signal already has a record for start time ${start.toString()}") as Throwable)
                // Don't have that start time
                signalStats.put(start, timeStats)
            }
        }
    }
}