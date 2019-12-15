import com.falkonry.data.utils.CSVParser
import java.io.File
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

fun main(){
    // Read file with CSV Parser
    val fpath="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\ToSplit\\2016-2019_header.csv"

    // Lambdas
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
        else if(name=="time"){
            var parts=colVal.split("/",":"," ")
            "${parts[2]}-${if(parts[0].length<2) "0" else ""}${parts[0]}-${if(parts[1].length<2) "0" else ""}${parts[1]} "+
                    "${if(parts[3].length<2) "0" else ""}${parts[3]}:${parts[4]}:${parts[5]}"
        }
        else if(colVal=="null") // Remove the word null
            null
        else
            colVal

    }

    // Construct parser
    var parser=CSVParser(File(fpath),tz= ZoneId.of("Europe/Amsterdam"),frmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),colHandler = columnHandler)


    // Prepare to write 3 files - autoclaves 6,7 and 8
    var f6="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\ToSplit\\2016-2019_6autoclave.csv"
    var f7="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\ToSplit\\2016-2019_7autoclave.csv"
    var f8="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\ToSplit\\2016-2019_8autoclave.csv"

    var cols=parser.cols.map{c->c.name}

    // Create headers mappings for files
    var maps6=cols.withIndex().filter{it.value.startsWith("020_Autoclave6_")}.associate{it.index to it.value.substring(it.value.indexOf("clave6_")+7)}
    var maps7=cols.withIndex().filter{it.value.startsWith("020_Autoclave7_")}.associate{it.index to it.value.substring(it.value.indexOf("clave7_")+7)}
    var maps8=cols.withIndex().filter{it.value.startsWith("020_Autoclave8_")}.associate{it.index to it.value.substring(it.value.indexOf("clave8_")+7)}
    var header6="time,entity,"+maps6.values.joinToString(",")
    var header7="time,entity,"+maps7.values.joinToString(",")
    var header8="time,entity,"+maps8.values.joinToString(",")


    File(f6).printWriter().use{
        var f6out=it
        f6out.println(header6)
        File(f7).printWriter().use{
            var f7out=it
            f7out.println(header7)
            File(f8).printWriter().use{
                var f8out=it
                f8out.println(header8)
                var rowCount=1
                parser.useDataRows { row, ex ->
                    if (row != null) {
                        // prepare to write a line to each file
                        var line6:StringBuilder=StringBuilder("")
                        var line7:StringBuilder=StringBuilder("")
                        var line8:StringBuilder=StringBuilder("")
                        var cnt6=0
                        var cnt7=0
                        var cnt8=0
                        for((i,col) in cols.withIndex()) {
                            var v=parser.getValueByColName(col,row)
                            // Write time and entity
                            if(col=="time")
                            {
                                line6.append(v.value as Instant?).append(",").append("Autoclave6").append(",")
                                line7.append(v.value as Instant?).append(",").append("Autoclave7").append(",")
                                line8.append(v.value as Instant?).append(",").append("Autoclave8").append(",")
                            }
                            else if(maps6.keys.contains(i))
                            {
                                if(v.value is Double?)
                                    line6.append(v.value as Double?)
                                else
                                    line6.append(v.value as Float?)
                                if(++cnt6<maps6.size)
                                    line6.append(",") // Add comma until las column
                            }
                            else if(maps7.keys.contains(i))
                            {
                                if(v.value is Double?)
                                    line7.append(v.value as Double?)
                                else
                                    line7.append(v.value as Float?)
                                if(++cnt7<maps7.size)
                                    line7.append(",") // Add comma until las column
                            }
                            else if(maps8.keys.contains(i))
                            {
                                if(v.value is Double?)
                                    line8.append(v.value as Double?)
                                else
                                    line8.append(v.value as Float?)
                                if(++cnt8<maps8.size)
                                    line8.append(",") // Add comma until las column
                            }
                        }
                        // Print the lines
                        f6out.println(line6)
                        f7out.println(line7)
                        f8out.println(line8)
                    } else if (ex != null)
                        println("Skipping row $rowCount due to Exception: ${ex.localizedMessage}")
                    rowCount++
                }
            }
        }
    }
}