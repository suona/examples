/** 
 * This project is licensed under the terms of the Modified BSD License
 * (also known as New or Revised or 3-Clause BSD), as follows:
 * 
 * Copyright (c) 2017, IBM Corporation. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT,INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.ibm.log

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source

/**
 * This Scala object will filter Syslog data and retrieve messages by type code
 */
object LogFilteringTypeCode
{   
    
    // Type code letter(s) (A,D,E,I,S,T,W)
    var typeCode = ""
    
    /**
     * Main method for filtering log data. Expects string of type code letters as argument.
     */
    def main(args: Array[String]) 
    {
        if (args.length != 1)
        {
            println("Usage:  spark-submit --class \"com.ibm.log.LogFilteringTypeCode\" --master local[4] jarFilename letter(s)")
            System.exit(0)
        }
        
        // Type code letter
        if (args(0).matches("[ADEISTW]{1,}"))
        {
            typeCode = args(0)
        }
        else
        {
            println("Must specify one or more type code letters (A,D,E,I,S,T,W), i.e. \"A\" or \"AE\"")
            System.exit(0)
        }
        
        // Create Spark session
        val spark = SparkSession.builder.getOrCreate()
        val sc=spark.sparkContext
        sc.setLogLevel("ERROR")
        
        // Create SQL context
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        import sqlContext.implicits._
        
        // Identify each line with indices
        var messageCount = 0
        var logData = sc.parallelize(Source.stdin.getLines.toList).filter(_.length > 56).zipWithIndex.map({case (l:String, i:Long) =>
            // If there is date and time listed, it is the start of a new message
            if (!l.substring(19, 26).trim().matches("")) 
            {
                // Update count of messages
                messageCount=i.toInt
            }
            // Map messages with index of message and index of line
            (messageCount,i,l)
        // Split lines into columns
        }).map({case (n:Int, i:Long, l:String) => 
          (n, i.toInt, l.substring(0,1).trim(), l.substring(1,2).trim(), 
              l.substring(2,9).trim(), l.substring(10,18).trim(), 
              l.substring(19,36).trim(), l.substring(37,45).trim(), 
              l.substring(46,54).trim(), l.substring(55).trim())}).toDF()
        
        // Rename columns so can access by name (instead of number)
        logData = logData.withColumnRenamed("_1", "MESSAGE_INDEX").withColumnRenamed("_2", "LINE_INDEX").withColumnRenamed("_3", "RECORD_TYPE").withColumnRenamed("_4", "REQUEST_TYPE").withColumnRenamed("_5", "ROUTE_CODES").withColumnRenamed("_6", "SYSTEM_NAME").withColumnRenamed("_7", "DATE_TIME").withColumnRenamed("_8", "JOBID").withColumnRenamed("_9", "MPF_FLAGS").withColumnRenamed("_10", "MESSAGE_TEXT")
        
        // Declare DataFrame object to hold our found rows
        var foundLines = null.asInstanceOf[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] 
        
        // Match pattern (Optional first character, optional ID field, then pattern of CCCnnns, CCCnnnns, CCCnnnnns or CCCSnnns)
        val pattern = "^([*@]{1}|.{0})([0-9]{0,} |.{0})([A-Z]{3,5}[0-9]{3,5}[" + typeCode + "].{0,})"
    
        // Filter lines that have pattern and are not continuations of previous lines
        foundLines = logData.filter($"MESSAGE_TEXT" rlike pattern).filter(!($"RECORD_TYPE" like "S"))
        
        // Extract only the indices of the found lines
        val foundIndices = foundLines.select("MESSAGE_INDEX").withColumnRenamed("MESSAGE_INDEX", "FOUND_INDEX").distinct
        
        // Retrieve all lines that are part of the messages indicated by the indices (sort by index)
        var foundMessages = logData.join(foundIndices,($"FOUND_INDEX" === $"MESSAGE_INDEX")).sort($"LINE_INDEX".asc)
        
        // Discard routing codes, record type, request type, and index columns
        foundMessages = foundMessages.drop("FOUND_INDEX").drop("MESSAGE_INDEX").drop("LINE_INDEX").drop("ROUTE_CODES").drop("RECORD_TYPE").drop("REQUEST_TYPE")
        
        // Display messages
        foundMessages.show(foundMessages.count.toInt, false)
    }
}