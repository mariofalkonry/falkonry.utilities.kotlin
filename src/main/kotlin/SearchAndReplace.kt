import java.io.File
import java.nio.file.Paths

fun main(args: Array<String>) {
    var search= arrayOf("[Europe/Paris]")
    var replace= arrayOf("")
    var rootDir="/Users/blackie/Documents/Projects/Falkonry/Aptar/November2019/ext_tunnel"
    var outDir="/Users/blackie/Documents/Projects/Falkonry/Aptar/November2019/ext_tunnel_cleaned"

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