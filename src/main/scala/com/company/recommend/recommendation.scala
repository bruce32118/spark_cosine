package com.company.recommend

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{rank, desc}
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory._
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory 
import collection.JavaConversions._
import collection.JavaConversions
import com.ruten.setting._
import com.ruten.recommend._
import com.ruten.util._

object Recommendation extends App with Tools{

    val RecommendSettingConf = new RecommendSetting

    val sqlContext = new org.apache.spark.sql.SQLContext(RecommendSettingConf.sc)
    
    import sqlContext.implicits._

    //讀入資料&只取tsid&gno => dataframe
    /*
    val raw_dataframe = RecommendSettingConf.sc
                        .textFile(RecommendSettingConf.filepath.getConfigData("spark_cosine_file_path"))
                        .map(x => x.split(","))
                        .map(x => (x(0),x(3))).toDF("tsid","gno")
*/

    val pp_pre = read_range_pre_data(RecommendSettingConf.sc, "xxxxx", "20170101", "20170130")

    import scala.util.Try

    def getValue( x : Option[String]): String = x match {
        case Some(data) => data
        case None => "NONE"
    }

    val raw_data = pp_pre.map(x => x.split("@")).filter(x => x.length == 6 && x(5).contains("/item/show?") ).map(x => {

        val tsid = getValue(Try(x(2).split("ts_id=")(1).split("&")(0)).toOption)
        val gno = getValue(Try(x(5).split("show\\?")(1)).toOption)

        (tsid.toString,gno.toString)

    }).filter(x => x._2 != "NONE").cache

    /*
    將資料讀入 並將資料存成(i,j),(j,k)格式
    ex. (i,j) = (1,2) 代表 在矩陣T1中 (1,2)這個位置 
    ex. (j,k) = (2,1) 代表 在矩陣T2中 (2,1)這個位置 

    matrix_right為matrix_left的轉置矩陣
    */ 

    val matrix_left = raw_data
                   .map(row =>(row._2, row._1))
                   .toDF("gno_left","tsid_left")

    val matrix_right = raw_data
                  .toDF("tsid_right","gno_right")
    
    val sql = RecommendSettingConf.sqlconfig.getConfigData("spark_cosine_groupby_matrix")

    val cosineSimilarityclass = new CosineSimilarity(sqlContext, matrix_left, matrix_right, 
        "gno_left", "gno_right", "tsid_left", "tsid_right", sql)


        cosineSimilarityclass.top10_aggr
        .rdd
        .map(x => new Tuple2((x.getString(0).hashCode%10 + 10).toString
                        , new Tuple2(x.getString(0), x.getSeq(1).mkString(","))))
        .groupByKey()
        .mapValues{value => JavaConversions.asJavaIterable(value)}
        .toJavaRDD()
        .foreach(
                //new berkeley writer
                 new BerkeleyDbSparkFactory.Write(
                        RecommendSettingConf.dbconfig.getConfigData("path")
                        , RecommendSettingConf.dbconfig.getConfigData("name"))
                )
}


