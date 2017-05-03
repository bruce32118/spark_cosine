package com.company.util

import java.util.Date
import java.util.Calendar  
import net.liftweb.json.JsonAST
import net.liftweb.json.Printer._
import net.liftweb.json.JsonDSL._
import java.net._
import org.joda.time.Duration
import org.joda.time.format._
import org.joda.time._


trait Tools{

         def now = {    
                
            val date = Calendar.getInstance

            import date._

            getTime   
         
         }

         def turnjson(inputdata : String) : String = {

            val json1 = ("time" -> now.toString) ~ ("gno" -> inputdata.split(",")(0)) ~ ("recommend_gno_list" -> inputdata.split(",")(1))
                            
            compact(JsonAST.render(json1))

         } 

               
          
         def getLocalIP = {

        	val localhost = InetAddress.getLocalHost  

        	localhost.getHostAddress

         }

  
        def fmt: org.joda.time.format.DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")

        //  merge multiple tslog files
        def read_range_date_data(sc: SparkContext, path: String, filename: String, start_date: String, end_date: String) : org.apache.spark.rdd.RDD[String] = {

            val start_date_parse = LocalDate.parse(start_date, fmt)
            val end_date_parse = LocalDate.parse(end_date, fmt)

            var rdd = sc.textFile(path + filename + "_" + fmt.print(start_date_parse) + ".csv")
            var current_date = start_date_parse
            if(current_date != end_date_parse){
                while (current_date.isBefore(end_date_parse)) {
                    current_date = current_date.plusDays(1)
                    rdd = rdd.union(
                        sc.textFile(path + filename + "_" + fmt.print(current_date) + ".csv")
                    )
                }
            }
        rdd
        }


        def read_range_pre_data(sc: SparkContext, path: String , start_date: String, end_date: String) : org.apache.spark.rdd.RDD[String] = {



            val start_date_parse = LocalDate.parse(start_date, fmt)

            val end_date_parse = LocalDate.parse(end_date, fmt)


            var rdd = sc.textFile(path + fmt.print(start_date_parse) + "/*")

            var current_date = start_date_parse

            if(current_date != end_date_parse){

                while (current_date.isBefore(end_date_parse)) {

                    current_date = current_date.plusDays(1)

                    rdd = rdd.union(

                        sc.textFile(path + fmt.print(start_date_parse) + "/*")

                    )

                }

            }

        rdd

        }



}
