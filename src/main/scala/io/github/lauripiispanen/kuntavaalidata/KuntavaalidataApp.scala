package io.github.lauripiispanen.kuntavaalidata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, LongType}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SaveMode

object KuntavaalidataApp {
  
  val spark = SparkSession.builder()
     .master("local[8]")
     .appName("Kuntavaalidata 2017")
     .config("spark.streaming.stopGracefullyOnShutdown",true)
     .getOrCreate()
     
  val sparkContext = spark.sparkContext
  
  def main(args: Array[String]) {
    import spark.sqlContext.implicits._
    
    val rawDataDF = loadData()
                      .filter($"ehdokas hyväksynyt" === 1)
                      .persist()
    
    // filter out comments
    val cols = rawDataDF.columns.filter { c =>      
      List("kunta", "id", "puolue", "sukunimi", "etunimi").contains(c) || c.matches("""^[0-9]+\|(?!kommentti).*""")
    }.map(c => rawDataDF(s"`$c`"))
    
    val municipalities = rawDataDF.select($"kunta").distinct().map(_.getString(0))
    val effectiveColumns = rawDataDF.select(cols:_*).persist()

    municipalities.collect().foreach { (municipality) =>      
      val candidates = effectiveColumns
              .filter($"kunta" === municipality)
              .map(Candidate(_))
              .persist()
     
      val matrix = new RowMatrix(candidates.rdd.map(_.answerVector))
      
      val componentsByIndex = matrix.multiply(matrix.computePrincipalComponents(2)).rows.zipWithIndex().map { case (components, index) => (index, components) }    
      val candidatesByIndex = candidates.rdd.zipWithIndex().map { case (candidate, index) => (index, candidate) }
     
      val results = candidatesByIndex.join(componentsByIndex).map {
        case (index, (candidate, vector)) =>
          Row(candidate.id, s"${candidate.lastName}, ${candidate.firstName}", candidate.party, vector(0), vector(1))
      }
      val outputSchema = StructType(
            StructField("id", StringType, false) ::
            StructField("name", StringType, false) ::
            StructField("party", StringType, false) ::
            StructField("x", DoubleType, false) ::
            StructField("y", DoubleType, false) :: Nil          
          )
      
      spark.createDataFrame(results, outputSchema)
           .write
           .format("json")
           .mode(SaveMode.Overwrite)
           .save(s"output/$municipality.json")
    }
    
    spark.stop()
  }
  
  def loadData() =
      spark.read.format("csv")
            .option("header", "true")
            .option("sep", ";")
            //.option("mode", "FAILFAST")
            .option("mode", "DROPMALFORMED")
            .load("src/main/resources/candidate_answer_data_20170405_kuntakysymykset.csv")
}

case class Candidate(municipality: String, id: String, lastName:String, firstName:String, party: String, answers: Array[String]) {
  def answerToDouble(answer:String) =
    Map("täysin samaa mieltä" -> 2.0,
        "jokseenkin samaa mieltä" -> 1.0,
        "jokseenkin eri mieltä" -> -1.0,
        "täysin eri mieltä" -> -2.0).get(answer)

  def answerVector = Vectors.sparse(answers.size, answers.map(answerToDouble).zipWithIndex.collect {
    case (Some(a), i) => (i, a)
  })
}
object Candidate {
  def apply(r: Row):Candidate = {
    Candidate(r.getString(0), 
              r.getString(1), 
              r.getString(2),
              r.getString(3),
              r.getString(4),
              r.toSeq.tail.tail.tail.tail.map(s => Option(s).map(_.toString).getOrElse("")).toArray)
  }
}