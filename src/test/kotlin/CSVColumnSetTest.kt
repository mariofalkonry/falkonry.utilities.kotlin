package com.falkonry.data.utils.test

import com.falkonry.data.utils.CSVColumn
import com.falkonry.data.utils.CSVColumnSet
import com.falkonry.data.utils.CSVValue
import org.junit.jupiter.api.Assertions.*
import java.lang.IndexOutOfBoundsException
import java.time.Instant
import java.time.ZonedDateTime
import kotlin.reflect.full.createType
import com.falkonry.data.InvalidTypeException

internal class CSVColumnSetTest {
    var testColumns= mutableListOf<CSVColumn>()
    var testColumnsDupName= mutableListOf<CSVColumn>()
    var testColumnsDupIndex= mutableListOf<CSVColumn>()

    @org.junit.jupiter.api.BeforeEach
    fun setUp() {
        var c1=CSVColumn("String1",0)
        var c2=CSVColumn("Int1",1, Int::class.createType(nullable=true))
        var c3=CSVColumn("DateTime1",2, Instant::class.createType(nullable=true))
        var c4=CSVColumn("Double1",3, Double::class.createType(nullable=true))
        testColumns.add(c1)
        testColumns.add(c2)
        testColumns.add(c3)
        testColumns.add(c4)
        var cSameName=CSVColumn("Double1",4,Double::class.createType(nullable=true))
        testColumnsDupName = testColumns.toMutableList()
        testColumnsDupName.add(cSameName)
        var cSameIndex=CSVColumn("Double2",1,Double::class.createType(nullable=true))
        testColumnsDupIndex = testColumns.toMutableList()
        testColumnsDupIndex.add(cSameIndex)
    }

    @org.junit.jupiter.api.AfterEach
    fun tearDown() {
        testColumns.clear()
        testColumnsDupName.clear()
        testColumnsDupIndex.clear()
    }

    @org.junit.jupiter.api.Test
    fun CSVValueType()
    {
        var sv=CSVValue("mario brenes")
        assertNotNull(sv)
        assertEquals(sv.type,String::class.createType(nullable = true))
        var dv=CSVValue(2.435)
        assertNotNull(dv)
        assertEquals(dv.type,Double::class.createType(nullable = true))
    }

    @org.junit.jupiter.api.Test
    fun CSVColumnConstructor()
    {
        var col=CSVColumn("mario brenes",1)
        assertNotNull(col)
    }

    @org.junit.jupiter.api.Test
    fun CSVColumnContructorBadName()
    {
        assertThrows(IllegalArgumentException::class.java) {
            var col=CSVColumn("",1)
        }
    }

    @org.junit.jupiter.api.Test
    fun CSVColumnContructorBadPos()
    {
        assertThrows(IllegalArgumentException::class.java) {
            var col=CSVColumn("mario brenes",-1)
        }
    }

    @org.junit.jupiter.api.Test
    fun CSVColumnContructorBadType()
    {
        assertThrows(InvalidTypeException::class.java) {
            var col=CSVColumn("Unsupported Type",0,ZonedDateTime::class.createType(nullable = true))
        }
    }

    @org.junit.jupiter.api.Test
    fun getCols() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        var cols=set.cols
        var names=cols.map { c->c.name }.toList()
        testColumns.map{c->c.name}.forEach() {
            assertTrue(names.contains(it))
        }
        var indexes=cols.map { c->c.pos }.toList()
        testColumns.map{c->c.pos}.forEach(){
            assertTrue(indexes.contains(it))
        }
    }

    @org.junit.jupiter.api.Test
    fun getNames() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        var names=set.names
        testColumns.map{c->c.name.toLowerCase()}.forEach() {
            assertTrue(names.contains(it))
        }
    }

    @org.junit.jupiter.api.Test
    fun getColumnByIndex() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        var col=set.getColumnByIndex(3)
        assertNotNull(col)
        col=set.getColumnByIndex(10)
        assertNull(col)
    }

    @org.junit.jupiter.api.Test
    fun getColumnByIndexBad() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertThrows(IllegalArgumentException::class.java) {
            var col=set.getColumnByIndex(-1)
        }
    }

    @org.junit.jupiter.api.Test
    fun getColumnByName() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        var col=set.getColumnByName("DateTime1")
        assertNotNull(col)
        col=set.getColumnByName("Not Existing")
        assertNull(col)
    }

    @org.junit.jupiter.api.Test
    fun getColumnByNameEmpty() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertThrows(IllegalArgumentException::class.java) {
            var col=set.getColumnByName("")
        }
    }

    @org.junit.jupiter.api.Test
    fun getIndexByName() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        var idx=set.getIndexByName("String1")
        assertEquals(idx,0)
    }

    @org.junit.jupiter.api.Test
    fun getIndexByNameEmpty() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertThrows(IllegalArgumentException::class.java) {
            var col=set.getIndexByName("")
        }
    }

    @org.junit.jupiter.api.Test
    fun getIndexByNameNotExist() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertThrows(IndexOutOfBoundsException::class.java) {
            var col=set.getIndexByName("Not existing")
        }
    }

    @org.junit.jupiter.api.Test
    fun add() {
        var set=CSVColumnSet()
        testColumns.forEach(){
            assertTrue(set.add(it))
        }
        assertEquals(set.size,testColumns.size)
    }

    @org.junit.jupiter.api.Test
    fun addSameName() {
        var set=CSVColumnSet()
        var idx=0
        testColumnsDupName.forEach(){
            if(idx++<4)
                assertTrue(set.add(it))
            else
                assertFalse(set.add(it))
        }
        assertEquals(set.size,testColumnsDupName.size-1)
    }

    @org.junit.jupiter.api.Test
    fun addSameIndex() {
        var set=CSVColumnSet()
        var idx=0
        testColumnsDupIndex.forEach(){
            if(idx++<4)
                assertTrue(set.add(it))
            else
                assertFalse(set.add(it))
        }
        assertEquals(set.size,testColumnsDupIndex.size-1)
    }

    @org.junit.jupiter.api.Test
    fun addAll() {
        var set=CSVColumnSet()
        assertTrue(set.addAll(testColumns))
        assertEquals(set.size,testColumns.size)
    }

    @org.junit.jupiter.api.Test
    fun addAllDupName() {
        var set=CSVColumnSet()
        assertFalse(set.addAll(testColumnsDupName))
        assertEquals(set.size,testColumnsDupName.size-1)
    }

    @org.junit.jupiter.api.Test
    fun addAllDupIndex() {
        var set=CSVColumnSet()
        assertFalse(set.addAll(testColumnsDupIndex))
        assertEquals(set.size,testColumnsDupIndex.size-1)
    }
    @org.junit.jupiter.api.Test
    fun clear() {
        var set=CSVColumnSet()
        var ret=set.addAll(testColumns)
        assertTrue(ret)
        set.clear()
        assertEquals(0,set.size)
    }

    @org.junit.jupiter.api.Test
    fun remove() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertEquals(set.size,testColumns.size)
        var i=0
        testColumns.forEach(){
            assertTrue(set.remove(it))
            i++
            assertEquals(set.size,testColumns.size-i)
        }
    }

    @org.junit.jupiter.api.Test
    fun removeAll() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertEquals(set.size,testColumns.size)
        assertTrue(set.removeAll(testColumns))
        assertEquals(set.size,0)
    }

    @org.junit.jupiter.api.Test
    fun retainAll() {
        // Pass exact list
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertFalse(set.retainAll(testColumns))  // None removed

        // Pass list of unknown elements
        var c1=CSVColumn("Double2",4,Double::class.createType(nullable=true))
        var c2=CSVColumn("Double3",5,Double::class.createType(nullable=true))
        var unknowns= mutableListOf<CSVColumn>()
        unknowns.add(c1)
        unknowns.add(c2)
        assertTrue(set.retainAll(unknowns))  // Everything removed
        assertEquals(set.size,0)

        // Pass list with only one element of the original
        set.addAll(testColumns) // Add again
        unknowns.add(testColumns[0])
        assertTrue(set.retainAll(unknowns))  // All but one element removed
        assertEquals(set.size,1)
    }

    @org.junit.jupiter.api.Test
    fun getSize() {
        var set=CSVColumnSet()
        set.addAll(testColumns)
        assertEquals(set.size,testColumns.size)
    }

    @org.junit.jupiter.api.Test
    fun isEmpty() {
        var set=CSVColumnSet()
        assertTrue(set.isEmpty())
        set.addAll(testColumns)
        assertFalse(set.isEmpty())
    }

    @org.junit.jupiter.api.Test
    operator fun iterator() {
    }

    @org.junit.jupiter.api.Test
    fun containsAll() {
        var set=CSVColumnSet()
        testColumns.forEach(){
            assertTrue(set.add(it))
        }
        assertTrue(set.containsAll(testColumns))
    }

    @org.junit.jupiter.api.Test
    fun containsAllDupName() {
        var set=CSVColumnSet()
        testColumnsDupName.forEach(){
            set.add(it)
        }
        assertFalse(set.containsAll(testColumnsDupName))
    }

    @org.junit.jupiter.api.Test
    fun containsAllDupIndex() {
        var set=CSVColumnSet()
        testColumnsDupIndex.forEach(){
            set.add(it)
        }
        assertFalse(set.containsAll(testColumnsDupIndex))
    }

    @org.junit.jupiter.api.Test
    fun contains() {
        var set=CSVColumnSet()
        testColumns.forEach(){
            assertTrue(set.add(it))
            assertTrue(set.contains(it))
        }
    }

    @org.junit.jupiter.api.Test
    fun containsDupName() {
        var set=CSVColumnSet()
        var idx=0
        testColumnsDupName.forEach()
        {
            set.add(it)
            if(idx++<4)
                assertTrue(set.contains(it))
            else
                assertFalse(set.contains(it))
        }
    }

    @org.junit.jupiter.api.Test
    fun containsDupIndex() {
        var set=CSVColumnSet()
        var idx=0
        testColumnsDupIndex.forEach()
        {
            set.add(it)
            if(idx++<4)
                assertTrue(set.contains(it))
            else
                assertFalse(set.contains(it))
        }
    }
}