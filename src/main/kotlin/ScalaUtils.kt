package com.falkonry.data.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import scala.collection.JavaConverters
import scala.collection.Seq

class ScalaUtils {
    companion object {
        fun getScalaColumnSeq(array:Array<String>): Seq<Column>
        {
            var cols:Array<Column> = array.map{ c-> functions.col(c) }.toTypedArray()
            return JavaConverters.asScalaIteratorConverter(cols.iterator()).asScala().toSeq()
        }

        fun <T> getScalaSeq(array:Array<T>): Seq<T>
        {
            return JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()
        }


    }
}