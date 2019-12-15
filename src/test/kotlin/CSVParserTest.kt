package com.falkonry.data.utils.test

import com.falkonry.data.SupportedType
import com.falkonry.data.utils.CSVParser
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.io.File
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneId
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType

internal class CSVParserTest {

        private val resourcesDirectory: String = "src/test/resources";

    @Test
    fun constructorTest() {
        // Create parser
        var parser = CSVParser(File(Paths.get(resourcesDirectory, "hasheader.csv").toUri()))
        assertNotNull(parser)
    }

    @Test
    fun getCols() {
        // Create parser
        var parser = CSVParser(File(Paths.get(resourcesDirectory, "hasheader.csv").toUri()))
        var cols = parser.cols
        assertEquals(5, cols.size)
        var dcol = cols.getColumnByName("double")
        assertNotNull(dcol)
        // Chooses the minimum sized e.g. float
        assertEquals(dcol!!.type, Float::class.createType(nullable = true))
        var scol = cols.getColumnByName("string")
        assertNotNull(scol)
        assertEquals(scol!!.type, String::class.createType(nullable = true))
        var dtcol = cols.getColumnByName("datetime")
        assertNotNull(dtcol)
        assertEquals(dtcol!!.type, Instant::class.createType(nullable = true))
        var lcol = cols.getColumnByName("long")
        assertNotNull(lcol)
        // Chooses the minimum sized e.g. Short
        assertEquals(lcol!!.type, Short::class.createType(nullable = true))
        var bcol = cols.getColumnByName("short")
        assertNotNull(bcol)
        assertEquals(bcol!!.type, Short::class.createType(nullable = true))
    }

    @Test
    fun useDataRows() {
        // Create parser
        var parser = CSVParser(File(Paths.get(resourcesDirectory, "hasheader.csv").toUri()),tz= ZoneId.of("UTC"))
        parser.useDataRows {r,ex->
            assertNull(ex)
            assertNotNull(r)
            val row=r!!
            assertTrue(row.pos > 0) // Skipped header
            when(row.pos)
            {
                // -10,text1,1959-01-01T00:00:00.0Z,,1
                1 -> {
                    assertEquals(-10f,row.values[0].value) // Is a double now
                    assertEquals("text1",row.values[1].value)
                    assertEquals(SupportedType.getInstant("1959-01-01T00:00:00.0Z"),row.values[2].value)
                    assertNull(row.values[3].value)
                    assertEquals(1.toShort(),row.values[4].value) // Short wins
                    // https://docs.oracle.com/javase/7/docs/api/java/lang/Boolean.html
                }
                // 1,text2,1959-01-20Z,3,0
                2 -> {
                    assertEquals(1f,row.values[0].value)
                    assertEquals("text2",row.values[1].value)
                    assertEquals(SupportedType.getInstant("1959-01-20T08:00:00Z"),row.values[2].value)
                    assertEquals(3.toShort(),row.values[3].value)
                    assertEquals(0.toShort(),row.values[4].value) // Anything but true is converted to booean false
                }
                // 2.945,,1959-01-31T00:00:00Z,4,true
                3 -> {
                    assertEquals(2.945f,row.values[0].value)
                    assertNull(row.values[1].value)
                    assertEquals(SupportedType.getInstant("1959-01-31T00:00:00Z"),row.values[2].value)
                    assertEquals(4.toShort(),row.values[3].value)
                    assertEquals((-2).toShort(),row.values[4].value)
                }
                // 3,text3,,-10,false
                4->{
                    assertEquals(3f,row.values[0].value)
                    assertEquals("text3",row.values[1].value)
                    assertNull(row.values[2].value)
                    assertEquals((-10).toShort(),row.values[3].value)
                    assertEquals((-1).toShort(),row.values[4].value)
                }
                // ,31days,2019-08-01 23:59:35,-1234,
                else->{
                    assertNull(row.values[0].value)
                    assertEquals("31days",row.values[1].value)
                    assertEquals(SupportedType.getInstant("2019-08-01T23:59:35Z"),row.values[2].value)
                    assertEquals((-1234).toShort(),row.values[3].value)
                    assertNull(row.values[4].value)
                }
            }
        }
    }

    @Test
    fun getValueByColName() {
        // Create parser
        var parser = CSVParser(File(Paths.get(resourcesDirectory, "hasheader.csv").toUri()))
        parser.useDataRows { r,ex->
            assertNotNull(r)
            val row=r!!
            when(row.pos)
            {
                // -10,text1,1959-01-01 00:00:00.0,,1
                1 -> {
                    assertEquals(-10f,parser.getValueByColName("double",row).value) // Is a double now
                }
                // 1,text2,1959-01-20,3,0
                2 -> {
                    assertEquals("text2",parser.getValueByColName("string",row).value)
                }
                // 2.945,,1959-31-01 00:00:00.0,4,true
                3 -> {
                    assertEquals((-2).toShort(),parser.getValueByColName("short",row).value)
                }
                // 3,text3,,-10,false
                4->{
                    assertEquals((-10).toShort(),parser.getValueByColName("long",row).value)
                }
                // ,31days,2019-08-01 23:59:35,-1234,
                else->{
                    assertEquals(SupportedType.getInstant("2019-08-01 23:59:35"),parser.getValueByColName("datetime",row).value)
                }
            }
        }
    }
}