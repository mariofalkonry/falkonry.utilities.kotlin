package com.falkonry.aptar

import com.falkonry.data.utils.CSVParser
import com.falkonry.data.utils.CSVValue
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import java.io.File
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class AptarColmec:CliktCommand(help="Aptar Brecey Colmec File Processor"){
    val rootDir by argument(help="Root directory containing files to load.  Note this directory will be traversed looking for *.csv files inside of 'Htc' subfolders")
    val ef by option(help="Csv file containing mapping of PLCs to file names. Default: 'COLMECvars.csv'").default("COLMECvars.csv")
    override fun run ()
    {
        var excludes= arrayOf("\$RT_OFF\$","\$RT_DIS\$")  // Not really signals

        // Files from Colmec to read - the tag name is in the file name
        //var rootDir="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC_Data_Oct"
        //var rootDir="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\-D_20_9_19-H_9_6_13"
        var outDir="$rootDir\\Output"
        //var entityFile="C:\\Users\\m2bre\\Documents\\Projects\\Aptar\\Data\\Colmec\\COLMEC vars.csv"

        var dirFilter:String="Htc" // Only process files in this subdirectory
        var fileFilter:String?=null
        var signalColumn:String="VarName"
        var timeColumn:String="TimeString"
        var entityColumn:String?=null
        var valCol:String="VarValue"
        var entityName:String?=null
        var timeZone:String="Europe/Paris"
        var dtFormat:String="yyyy-MM-dd HH:mm:ss"

        /* Lambda Functions */
        // Process column values
        val columnHandler: (String,String?) -> String? = {
                name,colVal ->
            if(colVal==null) {
                colVal
            }
            // Force all numeric values to Double
            else if(colVal.matches("-?\\d+([.,]\\d+)?([Ee][+-]0?\\d+)?".toRegex())) {
                if(colVal.contains("[.,]".toRegex()))
                    colVal.replace(",", ".")
                else
                    "$colVal.0"
            }
            else if(name==timeColumn){
                var parts=colVal.split(".",":"," ")
                "${parts[2]}-${parts[1]}-${parts[0]} ${parts[3]}:${parts[4]}:${parts[5]}"
            }
            else
                colVal

        }
        // File names
        var outFileNameGet:(File)->String={f->f.name}
        var outFileSubDirectory: (File)->String={f->f.parentFile.parentFile.name} // ... /htc

        // Delete output from previous time
        File(outDir).walk().forEach {
            if(it.isFile)
                it.delete()
        }

        // Load entities
        var entities=loadEntityMap(ef)

        // Walk through each file parsing
        File("$rootDir").walk().filter { it.path.contains(dirFilter) && it.name.endsWith(".csv")
                && !it.path.startsWith(outDir)  // in case nested outDir
                && !it.name.endsWith("_signal.csv")}.forEach {
            var fin=it
            println("Processing file ${it.path}")
            // Create signals data tracking
            var signals= mutableMapOf<String,SignalStats>()
            // Get entity name
            entityName=entities[fin.name.toLowerCase().substring(0,fin.name.lastIndexOf('_'))]

            // Process contents of the file
            fin.bufferedReader().use {
                // Open output file stream
                var reader = it
                var foutDir = outDir + "\\" + entityName.escapeForwardSlash() + "\\"+ outFileSubDirectory(fin)
                var filePathOut = foutDir + "\\" + outFileNameGet(fin)
                if (!File(foutDir).exists())
                    File(foutDir).mkdirs()

                // Parse it
                var parser = CSVParser(fin, sep = ';', colHandler = columnHandler, quote = '"',tz= ZoneId.of(timeZone),frmt = DateTimeFormatter.ofPattern(dtFormat))

                // Add synthetic column Entity_Signal
                parser.addSynthethicColumn("Entity_Signal",{r-> CSVValue(entityName+"_"+parser.getValueByColName(signalColumn,r).value.toString())})

                // Add synthetic column Entity
                parser.addSynthethicColumn("Entity",{CSVValue(entityName)})

                // Then save it with new columns, no semicolons or comma punctuation
                File("$filePathOut").printWriter().use {
                    // Write header
                    val fout = it
                    // Write header
                    fout.print(parser.getHeaderLine(newSep = ','))
                    parser.useDataRows { row, ex ->
                        if (row != null) {
//                            println(row.pos)
                            var signal=parser.getValueByColName(signalColumn,row).value as? String
                            // Exclude funny rows that are not really signals
                            if(!excludes.contains(signal)) {
                                var ts = parser.getValueByColName(timeColumn, row).value as? Instant
                                updateSignalStats(signals, signal!!, ts!!)
                                fout.write(parser.getDataRowLine(row, newSep = ','))
                            }
                        } else if (ex != null)
                            println("Skipping row due to Exception: ${ex.localizedMessage}")
                    }
                }
            }
            saveStats(entityName!!,fin.path,signals)
        }

        // After done with all files save to csv file
        var fout = outDir + "\\stats.csv"
        var file = File(fout)
        file.printWriter().use {
            it.println("entity,signal,start,end,count,file")
            for (entity in stats) {
                for (signal in entity.value) {
                    for (startTs in signal.value) {
                        it.println("${entity.key},${signal.key},${startTs.key},${startTs.value.end},${startTs.value.count},${startTs.value.file}")
                    }
                }
            }
        }
    }
}

fun main(args: Array<String>)=AptarColmec().main(args)

