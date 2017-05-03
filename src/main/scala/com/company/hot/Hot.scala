package com.company.hot

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{rank, desc}
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory._
import com.ruten.tools.berkeley.BerkeleyDbSparkFactory 
import collection.JavaConversions._
import collection.JavaConversions
import com.company.setting._
import com.company.recommend._
import com.company.util._
import java.time.LocalDate
import org.apache.spark.sql.expressions.Window

object Hot extends App with Tools{
   
    val HotSettingConf = new HotSetting

    val sqlContext = new org.apache.spark.sql.SQLContext(HotSettingConf.sc)
    
    import sqlContext.implicits._

    val yesterday = LocalDate.now().minusDays(1).toString.replace("-","").toString

    val pp_startday = LocalDate.now().minusDays(30).toString.replace("-","").toString

    val sold_record_startday = LocalDate.now().minusDays(60).toString.replace("-","").toString

    
    //讀取交易資料
    val pp_pre = read_range_date_data(HotSettingConf.sc, HotSettingConf.filepath.getConfigData("hot_file_path"), "PP_SOLD_TRANS", pp_startday, yesterday)

    val pp_header = pp_pre.first()

    val pp = pp_pre.filter(x => x != pp_header).map(x => x.split(",")).map(x => (x(1),x(5),x(6))).toDF("g_no","ctrl_no","g_num")

    //讀取下標資料
    val sold_pre = read_range_date_data(HotSettingConf.sc, HotSettingConf.filepath.getConfigData("hot_file_path"), "SOLD_RECORD", sold_record_startday, yesterday)

    val sold_header = sold_pre.first()

    val sold = sold_pre.filter(x => x != sold_header).map(x => x.split(",")).map(x => (x(2).toString,x(3).toString.substring(0,4))).toDF("ctrl_no","g_class")


    //join pp & sold_record table to get gclass
    val data_to_class = pp.join(sold, Seq("ctrl_no"), "left_outer").where($"g_class" !== "null").where($"g_class" !== "0025")
   
    val byGclassDesc = Window.partitionBy('g_class).orderBy('g_num desc)

    val rankByGnum = rank().over(byGclassDesc)

    //取每個分類top30
    val top30hot = data_to_class.select('*, rankByGnum as 'rank).where($"rank" < 30 )

    val recommend_hot_gno = top30hot.groupBy(col("g_class"))
                     .agg(collect_list("g_no") as "recommend_hot_gno")



    recommend_hot_gno
    .rdd
    .map(x => new Tuple2(x.getString(0).toString
                        , new Tuple2(x.getString(0), x.getSeq(1).mkString(","))))
    .groupByKey()
    .mapValues{value => JavaConversions.asJavaIterable(value)}
    .toJavaRDD()
    .foreach(
            //new berkeley writer
        new BerkeleyDbSparkFactory.Write(
                HotSettingConf.dbconfig.getConfigData("hot_path")
                , HotSettingConf.dbconfig.getConfigData("hot_name"))
             )



}
