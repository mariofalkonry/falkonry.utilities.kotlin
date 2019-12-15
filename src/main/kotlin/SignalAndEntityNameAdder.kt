import java.io.File

fun main(args: Array<String>) {
    // Files from PI to read - the tag name is in the file name
    // Examples: 20272PI.PV.csv, 90272PDI.PV.csv, etc.
    var fileDir="C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data"

    // Create a file with a map of signals that contains entity-->signal-->tag map
    var signalMapFile="C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\signalmaps.csv"
    /** Example - PLEASE CHECK WITH SME to complete this file. What I show here is only an example
     * and may not even be correct for the files received so far.  This is just for illustration
    entity,signal,tag
    Compressor1,InletTemp,10272TI.PV
    Compressor2,InletTemp,10272PI.PV
    Compressor1,InletPress,20272TI.PV
    Compressor2,InletPress,20272PI.PV
    Compressor1,MotorSpeed,10272SI.PV
    Compressor1,FilterDiffPress,90272PDI.PV
    Compressor2,FilterDiffPress,90272PDI.PV
    Compressor1,Flow,10272FI.PV
        ... some signals are common to both and hence they must be repeated, e.g. 90272PDI.PV
        ... you should have same signal names for each entity (tags may be different)
        ... if an entity has less signals than another, model can only be trained with common signal OR
        ... different models will be required for each entity
     **/

    // Process map file
    // Entity->Tag->Signal
    var signals=getSignalMap(File(signalMapFile))

    /* Lambda Functions */
    // The tag name is the name of the file minus .CSV
    var tagNameGet:(File)->String={f->f.name.substring(0,f.name.indexOf(".csv"))}
    // The output files that will be created are the same name a exist plus _signal
    var fileNameGet:(File)->String={f->"${f.path.substring(0,f.path.indexOf(".csv"))}_signal.csv"}


    // Header to put in every file
    var header="timestamp,value,entity,signal"


    File("$fileDir").walk().filter { it.name.endsWith(".csv") && !it.name.endsWith("_signal.csv")}.forEach {
        var fin=it
        fin.bufferedReader().use {
            // Open output file stream
            var reader=it
            var filePathOut=fileNameGet(fin)
            if(File("$filePathOut").exists())
                File("$filePathOut").delete()
            // Get tag name
            var tag=tagNameGet(fin)
            File("$filePathOut").printWriter().use {
                // Write header
                val fout=it
                // Write header
                fout.println(header)
                // Write lines until lenEach exceeded or no more lines to write (last file)
                var line:String?=null
                while ({ line = reader.readLine(); line }() != null) {
                    // Process data for each entity
                    for(kv in signals) {
                        var entity=kv.key
                        if(kv.value.containsKey(tag))
                            fout.println("$line,$entity,${kv.value[tag]}")
                    }
                }
            }
        }
    }
}

// Create map from file
private fun getSignalMap(mapFile:File):MutableMap<String,MutableMap<String,String>>
{
    var map= mutableMapOf<String,MutableMap<String,String>>()
    var header = emptyMap<String, Int>()
    var row = 0
    var firstDataRow=1
    mapFile.bufferedReader().lines().forEach {
        // Get header (upper case)
        if (header.isEmpty()) {
            var idx = 0
            header = it.split(",").map { Pair(it.toUpperCase(), idx++) }.toMap()
            if (!header.containsKey("ENTITY"))
                throw Exception("Missing ENTITY column in map file ${mapFile.path}")
            if (!header.containsKey("SIGNAL"))
                throw Exception("Missing SIGNAL column in map file ${mapFile.path}")
            if (!header.containsKey("TAG"))
                throw Exception("Missing TAG column in map file ${mapFile.path}")
        }
        if (row++ >= firstDataRow) {
            // Get map
            var data = it.split(",").toList()
            var entity=data[header["ENTITY"]!!]
            var tag=data[header["TAG"]!!]
            var signal=data[header["SIGNAL"]!!]
            if(map.containsKey(entity))
            {
                if(map[entity]!!.containsKey(tag))
                    throw Exception("Tag $tag is repeated for entity $entity in row $row of file ${mapFile.path}")
                map[entity]!!.put(tag,signal)
            }
            else
            {
                map.put(entity, mutableMapOf(Pair(tag,signal)))
            }
        }
    }
    return map
}