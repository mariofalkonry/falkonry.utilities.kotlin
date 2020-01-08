import java.io.File

fun main(args: Array<String>) {
    val path="/Users/blackie/Documents/code/KNIME/data/esp/d3.csv"
    val hasHeader=true
    var count=0

    File(path).readLines().forEach{
        count++
    }
    println("File ${path} has ${count-(if(hasHeader) 1 else 0)}")
}
