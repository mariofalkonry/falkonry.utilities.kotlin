package com.falkonry.data.utils

import com.b2rt.data.SupportedType
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.jvm.jvmErasure

class CSVColumn(name:String, pos:Int, type:KType = String::class.createType(nullable=true))
{
    var name:String = name
        private set(value){
            if(value.isNullOrEmpty())
            {
                throw IllegalArgumentException("Name must be a valid non zero length string")
            }
            field=value
        }

    var pos:Int=pos
        private set(value){
            if(value<0)
            {
                throw IllegalArgumentException("Pos must be >=0")
            }
            field=value
        }

    var type: KType = type
        private set(value)
        {
            if(SupportedType.isSupported(value.jvmErasure.java))
            {
                field=value
            }
        }

    init {
        this.name=name
        this.pos=pos
        this.type=type
    }
}