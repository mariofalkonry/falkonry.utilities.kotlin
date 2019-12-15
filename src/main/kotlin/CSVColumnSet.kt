package com.falkonry.data.utils

import java.lang.IllegalArgumentException

class CSVColumnSet:MutableSet<CSVColumn>
{
    var cols:MutableSet<CSVColumn> = mutableSetOf()

    var names:MutableSet<String> = mutableSetOf()

    private var indices:MutableSet<Int> = mutableSetOf()

    /* Methods */
    fun getColumnByName(name:String):CSVColumn?
    {
        if(name.isNullOrEmpty())
            throw(IllegalArgumentException("Name cannot be zero length string"))
        var lname=name.toLowerCase()
        if(!names.contains(lname))
            return null
        return cols.filter{c->c.name.toLowerCase()==lname}.firstOrNull()
    }

    fun getColumnByIndex(index:Int):CSVColumn?
    {
        if(index<0)
            throw(IllegalArgumentException("Index cannot be less than zero"))
        if(!indices.contains(index))
            return null
        return cols.filter{c->c.pos==index}.firstOrNull()
    }

    fun getIndexByName(name:String):Int
    {
        if(name.isNullOrEmpty())
            throw(IllegalArgumentException("Name cannot be zero length string"))
        var lname=name.toLowerCase()
        if(!names.contains(lname))
            throw(IndexOutOfBoundsException("Column with name $name not found"))
        return cols.filter{c->c.name.toLowerCase()==lname}.map{c->c.pos}.first()
    }

    /* Overriden methods */
    override fun add(element: CSVColumn): Boolean {
        var inName=element.name.toLowerCase()
        var inPos=element.pos
        if(!names.contains(inName) && !indices.contains(inPos)) {
            names.add(inName)
            indices.add(inPos)
            cols.add(element)
            return true
        }
        else
            return false
    }

    override fun addAll(elements: Collection<CSVColumn>): Boolean {
        var ret=true;
        elements.forEach() {
            if(!this.add(it))
                ret=false  // False if at least one element could not be added
        }
        return ret
    }

    override fun clear() {
        names.clear()
        cols.clear()
    }

    override fun remove(element: CSVColumn): Boolean {
        var inName=element.name.toLowerCase()
        var inPos=element.pos
        var ret=cols.removeIf { c -> c.name.toLowerCase() == inName && c.pos==inPos }
        if(ret) {
            names.remove(inName)
            indices.remove(inPos)
        }
        return ret
    }

    override fun removeAll(elements: Collection<CSVColumn>): Boolean {
        var ret=true
        elements.forEach() {
            if(!this.remove(it))
                ret=false // False if at least one element could not be removed
        }
        return ret
    }

    // Remove all else
    override fun retainAll(elements: Collection<CSVColumn>): Boolean {
        if(elements.isEmpty())
            return false
        var toRemove:MutableList<CSVColumn> = mutableListOf()
        cols.forEach()
        {
            var col=it
            var found=false
            for(toCheck in elements) {
                if(col.name.toLowerCase()==toCheck.name.toLowerCase() && col.pos==toCheck.pos) {
                    found=true
                    break
                }
            }
            if(!found) {
                toRemove.add(col)
            }
        }

        toRemove.forEach()
        {
            this.remove(it)
        }

        return toRemove.size>0
    }

    override val size: Int
        get()
        {
            return names.size
        }

    override fun isEmpty(): Boolean {
        return names.isEmpty()
    }

    override fun iterator(): MutableIterator<CSVColumn> {
        return this.cols.iterator()
    }

    override fun containsAll(elements: Collection<CSVColumn>): Boolean {
        if(elements.isEmpty())
            return false
        elements.forEach(){
            if(!names.contains(it.name.toLowerCase()) || !indices.contains(it.pos))
                return false
        }
        return true
    }

    override fun contains(element: CSVColumn): Boolean {
        return names.contains(element.name.toLowerCase()) && indices.contains(element.pos)
    }
}
