package com.ruten.setting
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.typesafe.config.ConfigFactory


class LoadConfSetting(val config_name : String){

  val config = ConfigFactory.load().getConfig(config_name)
   
  def getConfigData(tag_name : String) : String = config.getString(tag_name)

  //def getConfigData(config_name :String, tag_name: String) : String = ConfigFactory.load().getConfig(config_name).config.getString(tag_name)

  def SetLogger = {

	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("com").setLevel(Level.OFF)
	System.setProperty("spark.ui.showConsoleProgress","false")
	Logger.getRootLogger().setLevel(Level.OFF)

  }

}

class SparkConfSetting{

    val sparkconfig = new LoadConfSetting("spark")

    val conf = new SparkConf(true)
            .set("spark.driver.memory", sparkconfig.getConfigData("spark.driver.memory"))
            .set("spark.executor.memory", sparkconfig.getConfigData("spark.executor.memory"))
            .set("spark.executor.cores", sparkconfig.getConfigData("spark.executor.cores"))
            .set("spark.sql.crossJoin.enabled", "true")
}


class RecommendSetting{

    val dbconfig = new LoadConfSetting("db")    

    val sqlconfig = new LoadConfSetting("sql")

    val filepath = new LoadConfSetting("filepath")

    val conf = (new SparkConfSetting).conf.setMaster("local[*]")  

    val sc = new SparkContext(conf)    

}


class HotSetting{

    val dbconfig = new LoadConfSetting("db")    

    val sqlconfig = new LoadConfSetting("sql")

    val filepath = new LoadConfSetting("filepath")

    val conf = (new SparkConfSetting).conf.setMaster("local[*]")

    val sc = new SparkContext(conf)    

}