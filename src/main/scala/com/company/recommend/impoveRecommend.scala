package com.company.recommend

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{rank, desc}
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory._
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory 
import collection.JavaConversions._
import collection.JavaConversions
import com.company.setting._
import com.company.recommend._
import com.company.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.expressions.Window

class mow(raw_dataframe: DataFrame,sqlContext : SQLContext) {

    val c = raw_dataframe.rdd.map(x => (x(1).toString +","+ x(0).toString,1)).filter(x => x._1.split(",").length > 1 ).reduceByKey(_+_).map(x => (x._1.split(",")(0),x._1.split(",")(1),x._2))

    import sqlContext.implicits._
    val ad = c.toDF("gno","tsid","value")
    ad.registerTempTable("ad")
    val adnorm = ad.sqlContext.sql("select gno as gno_norm ,sum(value)*sum(value) as sumvalues_norm from ad group by gno").select("gno_norm","sumvalues_norm")

    val adjoinnorm = ad.join(adnorm,ad.col("gno") === adnorm.col("gno_norm")).select("gno","tsid","value","sumvalues_norm")

    
    val bd = adjoinnorm.toDF("gno_right","tsid_right","value_right","sumvalues_norm_right")
    

    val df_join  = adjoinnorm.join(bd,adjoinnorm.col("tsid") === bd.col("tsid_right")).where("gno != gno_right")


    df_join.registerTempTable("df")

    val df_cosine = df_join.sqlContext.sql("select gno, gno_right , sum(value) as sumvalues, sumvalues_norm, sumvalues_norm_right from df group by gno, gno_right, sumvalues_norm, sumvalues_norm_right")

    val cosine = df_cosine.withColumn("cosineSimilarity", df_cosine.col("sumvalues")/(df_cosine.col("sumvalues_norm")+df_cosine.col("sumvalues_norm_right")))

    val w = Window.partitionBy($"gno").orderBy(desc("cosineSimilarity"))
    //根據 cosineSimilarity 取出前十個recomnd gno
    val df_click_top10 = cosine
                         .withColumn("rank", rank.over(w))
                         .where($"rank" <= 10)


   }
  

class MyKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[mow])
  }
}

object impoveRecommend extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    val RecommendSettingConf = new RecommendSetting

    val sqlContext = new org.apache.spark.sql.SQLContext(RecommendSettingConf.sc)

    import sqlContext.implicits._

    //讀入資料&只取tsid&gno => dataframe

    val raw_dataframe = sqlContext.read.load("gs://brucelin/20170301to20170315.parquet")

   
    val x = new mow(raw_dataframe,sqlContext)

    

     x.df_click_top10.show()


}
