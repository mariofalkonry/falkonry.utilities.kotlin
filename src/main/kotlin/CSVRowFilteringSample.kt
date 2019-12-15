package com.falkonry.data.utils

import java.io.File

fun main(args: Array<String>) {
    /* Lambda Functions */
    // Process column values
    val columnHandler: (String,String?) -> String? = {
            name,colVal ->
        if(colVal==null) {
            colVal
        }
        // Force all numeric values to parser ignores them
        else if(colVal.matches("-?\\d+([.,]\\d+)?([Ee][+-]0?\\d+)?".toRegex())) {
                "a$colVal"
        }
        else
            colVal

    }



    var rootDir="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\Boolean Data\\Output"
    File("$rootDir").walk().filter(){it.name.endsWith(".csv")
            && !it.name.endsWith("_batches.csv")}.forEach {
        var inFile = it
        var colToFilter = "signal"
        var colFilters = arrayOf( "Program_Descriptor","BatchID_Calc","In_Process","Process_Complete")
        val parser = CSVParser(inFile,colHandler = columnHandler)
        val outfile = inFile.path.replace(".csv", "_batches.csv")
        File(outfile).bufferedWriter().use {
            var writer = it
            it.write(parser.getHeaderLine())
            parser.useDataRows { row, ex ->
                if (row != null) {
                    if (colFilters.contains(parser.getValueByColName(colToFilter, row).value.toString())) {
                        writer.write(parser.getDataRowLine(row))
                    }
                } else if (ex != null)
                    println("Exception retrieving row: ${ex.localizedMessage}")
            }
        }
    }
}