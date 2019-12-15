package com.falkonry.data.utils

import com.falkonry.data.SupportedType
import java.io.File
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.jvm.jvmErasure

class CSVParser(file: File,sep:Char=',',quote:Char?=null,hasHeader:Boolean=true,
                colHandler:(name:String,colVal:String?)->String?={name,colVal->colVal},
                tz:ZoneId=ZoneId.systemDefault(),frmt:DateTimeFormatter= DateTimeFormatter.ISO_DATE_TIME)
{
    private val BOM='\uFEFF'

    enum class DuplicateHandling {KEEPFIRST,KEEPLAST,KEEPNONE}

    private var file:File=file

    private var colHandler=colHandler

    private var tz=tz

    private var frmt=frmt

    private var sep:Char=sep

    private var quote:Char?=quote

    private var hasHeader:Boolean=hasHeader

    var cols:CSVColumnSet = CSVColumnSet()
    private set

    // private state variables
    private var linesToCheckForType:Int = 9

    // getRows stream-like
    fun useDataRows(callback:(CSVRow?,Exception?)->Unit )
    {
        var lineIdx=0
        file.bufferedReader().useLines {
            it -> it.forEach {
                if (!hasHeader || lineIdx>0) {
                    var line = it
                    var e:Exception?=null
                    var row:CSVRow?
                    try {
                        row=parseRow(lineIdx,it)
                    }
                    catch(ex:Exception)
                    {
                        row=null
                        e=ex
                    }
                    callback(row,e)
                }
                lineIdx++
            }
        }
    }

    // getValueByColName pass it a row
    fun getValueByColName(name:String,row:CSVRow):CSVValue
    {
        // Return synthetic value if exists
        // For synthetic columns store name but do not allow same in different case
        var lname=name.toLowerCase()
        var exist=synthCols.filter(){c->c.key.toLowerCase()==lname}.keys.firstOrNull()
        if(exist!=null){
            return synthCols[exist]!!(row)
        }
        // Else return from regular columns
        var idx=cols.getIndexByName(name)
        return row.values[idx]
    }

    // Get values by ColName passed
    fun getValuesByColNames(names:Array<String>,row:CSVRow):Array<CSVValue?>
    {
        var ret= arrayOfNulls<CSVValue?>(names.size)
        for(i in 0 until(names.size)) {
            ret[i]=try{
                getValueByColName(names[i],row)
            }
            catch (e:Exception){
                null
            }
        }
        return ret
    }

    // Get header
    fun getHeaderLine(cols: CSVColumnSet=this.cols,newSep:Char=this.sep):String
    {
        var line=cols.map{c->(quote?.toString()?:"")+c.name+(quote?.toString()?:"")}.joinToString(separator = newSep.toString())
        // Append synthethic columns
        var synth=synthCols.map{c->(quote?.toString()?:"")+c.key+(quote?.toString()?:"")}.joinToString(separator = newSep.toString())
        return line+newSep.toString()+synth+System.lineSeparator()
    }

    // Get data row line
    fun getDataRowLine(row:CSVRow,newSep:Char=this.sep):String
    {
        // Process row
        var line=row.values.map{v->(quote?.toString()?:"")+(v.value?.toString()?:"")+(quote?.toString()?:"")}.joinToString(separator = newSep.toString())
        // Append synthethic columns
        var synth=synthCols.map{c->(quote?.toString()?:"")+(c.value(row).value?.toString()?:"")+(quote?.toString()?:"")}.joinToString(separator = newSep.toString())
        return line+newSep.toString()+synth+System.lineSeparator()
    }

    // TODO: useDataRowsNoDuplicates(callback:(CSVRow)->Unit,dupfound:(CSVROw)->Unit,keyCols:List<String>)

    // TODO: useDataRowsPivot(callback:(CSVRow),dupfound: (CSVRow)->Unit,pivotCols:List<String>)


    var synthCols= mutableMapOf<String,(row:CSVRow)->CSVValue>()
    fun addSynthethicColumn(name:String,handler:(row:CSVRow)->CSVValue)
    {
        var lname=name.toLowerCase()
        if(cols.names.contains(lname))
            throw(UnsupportedOperationException("Column named $name already exists in regular columns"))
        // For synthetic columns store name but do not allow same in different case
        var exist=synthCols.filter(){c->c.key.toLowerCase()==lname}.keys.firstOrNull()
        if(exist==null)
            synthCols.put(name,handler)
        else
            synthCols[exist]=handler
    }

    // Load file
    init
    {
        // Process header
        file.bufferedReader().use {
            var lines= mutableListOf<String>()
            var line=it.readLine()
            if(line==null)
                throw(IndexOutOfBoundsException("file ${file.path} is empty"))
            if(lines.isEmpty()) // Strip BOM from first line
                line=line.trim(BOM)
            lines.add(line)

            // Collect first n lines after header for type inference
            var lineNum=if(!hasHeader) 1 else 0
            while(lineNum++ < linesToCheckForType) {
                line = it.readLine()
                if(line==null) {
                    if(lineNum<2 && hasHeader)
                        throw(IndexOutOfBoundsException("file ${file.path} has a header and no data rows"))
                    break
                }
                lines.add((line))
            }
            cols=parseColumns(lines)
        }
    }

    private fun parseRow(lineIdx:Int,line:String):CSVRow
    {
        var ret:MutableList<CSVValue> = mutableListOf()
        var data=splitLine(line)
        if(cols.size!=data.size)
            throw(IndexOutOfBoundsException("Number of columns=${data.size} in row $lineIdx does not match number of columns=${cols.size} in the header"))
        // Create CSVValues
        for(i in 0 until cols.size) {
            var v:CSVValue = CSVValue()  // null value type
            try {
                var colVal = colHandler(cols.getColumnByIndex(i)!!.name, data[i])
                // Skip trying to convert null columns simply return emtpy value
                if(!colVal.isNullOrEmpty()) {
                    // Handle Instant with TZ
                    if (cols.getColumnByIndex(i)!!.type.jvmErasure.java == Instant::class.java)
                        v = CSVValue(
                            SupportedType.getInstant(
                                colHandler(cols.getColumnByIndex(i)!!.name, data[i]),
                                tz,
                                frmt
                            )
                        )
                    else
                        v = CSVValue(
                            SupportedType.convert(
                                colHandler(cols.getColumnByIndex(i)!!.name, data[i]),
                                cols.getColumnByIndex(i)!!.type.jvmErasure.java
                            )
                        )
                }
            } catch (e: Exception) {
                throw(e)
                //               println(e.message)
                //               println(e.stackTrace)
            }
            ret.add(v)
        }
        return CSVRow(lineIdx,ret.toTypedArray())
    }

    private fun parseColumns(lines:List<String>):CSVColumnSet {
        var colTypes:MutableMap<Int,MutableList<KType>> = mutableMapOf() // Column list of candidate types
        var colNames:Array<String> = emptyArray()
        var lineIdx=0
        for(line in lines)
        {
            var data=splitLine(line)
            if(lineIdx++<1) {
                if(!hasHeader) {
                    var idx = 1
                    colNames = data.map { c -> "Column_" + idx++ }.toTypedArray()
                }
                else {
                    colNames = data
                    continue
                }
            }
            var colIdx=0
            if(colNames.size==data.size) {
                for ((i, col) in data.withIndex()) {
                    // To be safe force all numeric values to floating point values
                    var type = SupportedType.parse(colHandler(colNames[i], col))::class.createType(nullable = true)
                    //                var type=SupportedType.parse(col)::class.createType(nullable=true)  // Do what it can naturally, TODO: date time is confusing it !!!
                    if (colTypes.containsKey(colIdx)) {
                        colTypes[colIdx]!!.add(type)
                    } else {
                        colTypes.put(colIdx, mutableListOf(type))
                    }
                    colIdx++
                }
            }
        }

        // Create colums set
        var types=getMostLikelyTypes(colTypes)
        var ret=CSVColumnSet()
        for(i in 0..colNames.size-1)
        {
            var col=CSVColumn(colNames[i],i,types[i]!!)
            ret.add(col)
        }
        return ret
    }

    private fun getMostLikelyTypes(colTypes:MutableMap<Int,MutableList<KType>>):MutableMap<Int,KType> {
        var types:MutableMap<Int,KType> = mutableMapOf()
        colTypes.forEach {
            var col=it
            var colNo=it.key
            // If there is a double or float that wins
            if(col.value.contains(Double::class.createType(nullable=true))) {
                types.put(colNo,Double::class.createType(nullable=true))
            }
            else if(col.value.contains(Float::class.createType(nullable=true))){
                types.put(colNo,Float::class.createType(nullable=true))
            }
            // If there is a long or int or short that wins next
            else if(col.value.contains(Long::class.createType(nullable=true))){
                types.put(colNo,Long::class.createType(nullable=true))
            }
            else if(col.value.contains(Int::class.createType(nullable=true))){
                types.put(colNo,Int::class.createType(nullable=true))
            }
            else if(col.value.contains(Short::class.createType(nullable=true))){
                types.put(colNo,Short::class.createType(nullable=true))
            }
            // If there is a boolean that wins next
            else if(col.value.contains(Boolean::class.createType(nullable=true))){
                types.put(colNo,Boolean::class.createType(nullable=true))
            }
            // If there is an instant that wins next
            else if(col.value.contains(Instant::class.createType(nullable=true))){
                types.put(colNo,Instant::class.createType(nullable=true))
            }
            // Else majority that is not a string wins
            else {
                var typeCounts=col.value.groupBy { it }.map{e->Pair(e.value.size,e.key)}.sortedByDescending { it.first }.toMap()
                var maxCnt=0
                var maxType:KType=String::class.createType(nullable=true)
                for(type in typeCounts)
                {
                    if(type.key > maxCnt)
                    {
                        maxType=type.value
                        maxCnt=type.key
                    }
                    // If more than one have max count, anything other than string wins
                    else if(type.key==maxCnt)
                    {
                        if(maxType==String::class.createType(nullable=true))
                            maxType=type.value
                    }
                    else
                        break // Done searching from maximum
                }
                types.put(colNo,maxType)
            }
        }
        return types
    }

    private fun splitLine(line:String):Array<String>
    {
        var parts=line.split(sep.toString())
        var cleanParts=parts.map { it.trim({c->c==quote})}
        return cleanParts.toTypedArray()
    }
}
