### preinstall  


- java 8
- scala 2.11.8
- spark 2.0.2
- sbt 0.13.9


### quickstart  


 * clone the project

```
git clone http://10.240.0.3/bruce32118/spark_cosine.git
    
cd spark_cosine
```


#### 將project包成jar檔  


```
     
~/SBT_HOME/bin/sbt assembly(在spark_cosine資料夾下執行)
     
```


#### standalone模式(spark-cluster)(上線會採取這種模式)  

    
```
    
SPARK_HOME/spark-submit --class com.ruten.recommend.recommendation --master (spark master url) --jar ~/spark_cosine/target/scala-2.11/spark_cosine-assembly-1.0.jar.jar
    
```








=======
### preinstall  


- java 8
- scala 2.11.8
- spark 2.0.2
- sbt 0.13.9


### quickstart  


 * clone the project

```
git clone http://10.240.0.3/bruce32118/spark_cosine.git
    
cd spark_cosine
```


#### 將project包成jar檔  


```
     
~/SBT_HOME/bin/sbt assembly(在spark_cosine資料夾下執行)
     
```


#### standalone模式(spark-cluster)(上線會採取這種模式)  

    
```
    
SPARK_HOME/spark-submit --class com.ruten.recommend.recommendation --master (spark master url) --jar ~/spark_cosine/target/scala-2.11/spark_cosine-assembly-1.0.jar.jar
    
```

