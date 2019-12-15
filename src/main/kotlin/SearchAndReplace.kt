import java.io.File
import java.nio.file.Paths

fun main(args: Array<String>) {
    var search= arrayOf("000000","Comp-A")
    var replace= arrayOf("","C021A")
    var rootDir="C:\\Users\\m2bre\\Documents\\Falkonry\\Demos\\Source"
    var outDir="C:\\Users\\m2bre\\Documents\\Falkonry\\Demos\\Cleaned"

    // Delete output from previous time
    if(!File(outDir).exists())
        File(outDir).mkdir()
    else {
        File(outDir).walk().forEach {
            if (it.isFile)
                it.delete()
        }
    }

    // Go through all files
    File(rootDir).walk().filter{it.name.endsWith(".csv")}.forEach {
        var fin=it
        File(Paths.get(outDir,fin.name).toAbsolutePath().toUri()).printWriter().use {
            var fout=it
            fin.readLines().forEach {
                var newline=it
                for((i,s) in search.withIndex()){
                    newline=newline.replace(s,replace[i])
                }
                fout.println(newline)
            }
        }
    }
}