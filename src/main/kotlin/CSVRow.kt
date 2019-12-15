package com.falkonry.data.utils

class CSVRow constructor(pos:Int, values:Array<CSVValue>)
{
    // In order according to their column position
    var values=values
        private set

    var pos=pos
        private set
}
