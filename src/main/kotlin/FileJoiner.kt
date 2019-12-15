import java.io.File
import java.io.InvalidObjectException
import java.io.PrintWriter

fun main(args: Array<String>) {

    // Size threshold of joined files
    var maxSize=128*1024*1024


    var inFilesRoot="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\Boolean Data\\Output"
//    var outFileNameRoot = "C:\\Users\\m2bre\\Documents\\Projects\\Teekay\\Data\\PI Tag Extractions\\Joined"

    var outFileNameRoot = "C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\Batches"

    // Delete existing files or create directory
    if(File(outFileNameRoot).exists()) {
        File(outFileNameRoot).walk().forEach {
            if(it.isFile)
                it.delete()
        }
    }
    else
        File(outFileNameRoot).mkdir()

    // Go through files and join
//    var inFileNameFilters=getUniqueNames("_batches",inFilesRoot)
    var outFileName = "${outFileNameRoot}\\AutoclaveBatches.csv"
    var nfile=1
    var writing=false
    var size=0
    var fileWriter:PrintWriter?=null
    var header:String=""
    var firstFile=""
    File(inFilesRoot).walk().filter{it.name.endsWith("batches.csv")}.forEach  {
        var fileIn=it
        var firstLine=true
        fileIn.bufferedReader().forEachLine {
            var line=it
            // Open file to write if not already opened
            if (!writing) {
                var filePathOut = "${outFileName.replace(".", "_$nfile.")}"
                fileWriter = File("$filePathOut").printWriter()
                if(header.isNullOrEmpty())
                    header = line
                fileWriter!!.println(header) // Header
                writing = true
                size = header.toByteArray().size
            }
            // Skip header in first line of source file
            if (!firstLine) {
                fileWriter!!.println(line)
                size += line.toByteArray().size
            } else {
                firstLine = false
                if(firstFile.isNullOrEmpty())
                    firstFile=fileIn.name
                else {
                    var newHeader = line // Read new header and test to be the same
                    if (header.compareTo(newHeader) != 0) {
                        throw(InvalidObjectException(
                            "File ${fileIn.name} contains a different header that the first processed file $firstFile"
                                    + System.getProperty("line.separator")
                                    + "Original header=$header"
                                    + System.getProperty("line.separator")
                                    + "New header=$newHeader"
                        ))
                    }
                }
            }
        }

        // If the size is exceeded, close writer and reset flags
        if (size > maxSize) {
            fileWriter?.close()
            writing = false
            nfile++ // Move on to next output file
            firstFile="" // Reset first file flag
        }
    }
    // Close after last file is read anyways
    fileWriter?.close()
}

fun getUniqueNames(stopPattern:String,rootDir:String):MutableSet<String>{
    var uniques= mutableSetOf<String>()
    File(rootDir).walk().filter{it.name.endsWith(".csv") }.forEach {
        var idx=it.name.lastIndexOf(stopPattern)
        if(idx>-1) {
            var test = it.name.substring(0, it.name.lastIndexOf(stopPattern))
            if (!uniques.contains(test)) {
                uniques.add(test)
            }
        }
    }
    return uniques
}