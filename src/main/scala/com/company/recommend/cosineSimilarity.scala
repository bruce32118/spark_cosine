package com.company.recommend

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import com.ruten.setting._
import scala.math._
import org.apache.spark.sql.functions.udf
 

/*
    a dot b 實作方式

    a 實際資料為 (i,j,1)
    b 實際資料為 (j,k,1) ps.但其實b為a的轉至矩陣所以也可以寫成(j,i,1)
  
    a dot b => 其實就是一個自然連接運算+分組+聚合運算

    (i,j,1) (j,k,1) 有一個公用屬性 j

    若以j作關聯

    (i,j,k,1,1) => 這五個元素即為 a,b這兩個矩陣的元素對

    a dot b => 即為這兩個元素對的積 => (i,j,k,1*1)

    而j元素在做內積時會被消掉

    最後得到(i,k,1) end

    這段code主要是以spark dataframe的方式去處理資料
   
    附上別人用的方法
    
    def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {

        val M_ = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
        val N_ = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) })

        val productEntries = M_
            .join(N_)
            .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
            .reduceByKey(_ + _)
            .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

        new CoordinateMatrix(productEntries)
    }  
    */  


class CosineSimilarity(sqlContext : SQLContext, matrixA : DataFrame, matrixB : DataFrame, dataframe_column_gno_A : String, 
                          dataframe_column_gno_B : String, dataframe_column_tsid_A : String, dataframe_column_tsid_B : String, sql : String) extends Serializable{
    
    import sqlContext.implicits._

    //自訂義spark udf cosineSimilarity
    def cosineSimilarity_count(click:Double, gno_a_index:Double, gno_b_index:Double, tsid_a_index:Double) : Double = {

        val botom_a = gno_a_index*gno_a_index + tsid_a_index*tsid_a_index + click*click
    
        val botom_b = gno_b_index*gno_b_index + tsid_a_index*tsid_a_index + click*click
    
        val result = click / (scala.math.sqrt(botom_b.toInt)*scala.math.sqrt(botom_a.toInt))

      result
    }

    val cosineSimilarity_countUDF = udf(cosineSimilarity_count _)

    
    //將gno,tsid 加上index
    
    val dataframe_column_gno_A_Index = new StringIndexer()
               .setInputCol(dataframe_column_gno_A)
               .setOutputCol(dataframe_column_gno_A+"Index")
    
    val dataframe_column_gno_B_Index = new StringIndexer()
               .setInputCol(dataframe_column_gno_B)
               .setOutputCol(dataframe_column_gno_B+"Index")

    val dataframe_column_tsid_A_Index = new StringIndexer()
               .setInputCol(dataframe_column_tsid_A)
               .setOutputCol(dataframe_column_tsid_A+"Index")

    val dataframe_column_tsid_B_Index = new StringIndexer()
               .setInputCol(dataframe_column_tsid_B)
               .setOutputCol(dataframe_column_tsid_B+"Index")

   

    val matrixA_pipe = new Pipeline()
               .setStages(Array(dataframe_column_gno_A_Index, dataframe_column_tsid_A_Index))

    val pipeline_a = matrixA_pipe.fit(matrixA).transform(matrixA)

    val matrixB_pipe = new Pipeline()
               .setStages(Array(dataframe_column_gno_B_Index, dataframe_column_tsid_B_Index))

    val pipeline_b = matrixB_pipe.fit(matrixB).transform(matrixB)


    //join matrix_A & matrix_B

    val df_join = pipeline_a.join(pipeline_b, pipeline_a.col(dataframe_column_tsid_A) === pipeline_b.col(dataframe_column_tsid_B))

    //去除相同gno的商品(不需要算同個商品與同個商品的相似度) 並增加click欄位 
    val df = df_join
            .where(dataframe_column_gno_A + " != " + dataframe_column_gno_B)
            .withColumn("click",lit(1.toDouble))
    //暫存一個table df   
    df.registerTempTable("df")
    //groupBy gno_a & sum click
    val df_groupby_matrix_sum_click = df.sqlContext.sql(sql)
    // count cosineSimilarity
    val do_cosineSimilarity = df_groupby_matrix_sum_click.withColumn("cosineSimilarity", cosineSimilarity_countUDF('click,$"${dataframe_column_gno_A+"Index"}",$"${dataframe_column_gno_B+"Index"}",$"${dataframe_column_tsid_A+"Index"}"))


    //https://read01.com/ymJx.html 解釋 @transient
    //spark Window 不可序列化
    @transient val w = Window.partitionBy($"${dataframe_column_gno_A}").orderBy(desc("cosineSimilarity"))
    //根據 cosineSimilarity 取出前十個recomnd gno
    @transient val df_click_top10 = do_cosineSimilarity
                         .withColumn("rank", rank.over(w))
                         .where($"rank" <= 10)

    val top10_aggr = df_click_top10.groupBy(col(dataframe_column_gno_A))
                     .agg(collect_list(dataframe_column_gno_B) as "recommend_gno")


}



