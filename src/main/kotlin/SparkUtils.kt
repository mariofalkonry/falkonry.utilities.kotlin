package com.falkonry.data.utils

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

class SparkUtils
{
    class MyFirst: UserDefinedAggregateFunction()
    {
        // This is the input fields for your aggregate function.
        override fun inputSchema(): StructType {
            return StructType(arrayOf(StructField("value",DataTypes.StringType,true,Metadata.empty())))
         }

        // This is the internal fields you keep for computing your aggregate
        override fun bufferSchema(): StructType {
            return StructType(arrayOf(StructField("value",DataTypes.StringType,true,Metadata.empty())))
        }

        // This is the output type of your aggregatation function.
        override fun dataType(): DataType=DataTypes.StringType

        // Is deterministic
        override fun deterministic(): Boolean=true

        // This is the initial value for your buffer schema.
        override fun initialize(buffer: MutableAggregationBuffer?) {
            buffer?.let {
                buffer.update(0,null)
            }
        }

        // This is how to update your buffer schema given an input.
        override fun update(buffer: MutableAggregationBuffer?, input: Row?) {
            buffer?.let {
                buffer.update(0,buffer.getString(0))
            }
        }

        // This is how to merge two objects with the bufferSchema type.
        override fun merge(buffer1: MutableAggregationBuffer?, buffer2: Row?) {
            buffer1?.let {
                buffer1.update(0,buffer2?.getString(0)?:buffer1.getString(0))
            }
        }

        // This is where you output the final value, given the final value of your bufferSchema.
        override fun evaluate(buffer: Row?): Any {
            return buffer?.getString(0)?:""
        }
    }
}