package com.falkonry.data.utils

import kotlin.reflect.KType
import kotlin.reflect.full.createType

class CSVValue(v:Any?=null) {
    var value = v
        private set

    var type: KType? = getType(v)
        private set

    private fun getType(v:Any?): KType?
    {
        if(v==null)
            return null
        return v::class.createType(nullable=true)
    }
}