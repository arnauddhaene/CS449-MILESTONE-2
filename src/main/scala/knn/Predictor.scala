package knn

import similarity.Rating

import similarity.RatingFunctions._
import similarity.PairRDDFunctions._

import similarity.Predictor.cosineSimilarities
import similarity.Predictor.predictBySimilarity

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }).coalesce(1)
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }).coalesce(1)
  assert(test.count == 20000, "Invalid test data")

  // ######################## MY CODE HERE ##########################

  /**
    * Compute rating prediction using the cosine similarity method.
    *
    * @param train RDD[Rating]
    * @param test RDD[(Int, Int)]
    * @param similarities cosine similarities between user pairs
    * @param topK Top N similarities taken into account
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def cosineKnnPredictor(
    train : RDD[Rating], test : RDD[(Int, Int)], 
    similarities : IndexedSeq[((Int, Int), Double)], topK : Int):  RDD[Rating] = {

    val topKSimilarities = similarities
      .map { case ((u, v), s) => (u, v, s) }
      // this will generate a Map as ( u -> (u, v, s) )
      .groupBy(_._1)
      // will generate a Map ( (u, v) -> s )
      .flatMap { case (k, v) => 
        // Sort by descending similarity and take top k values
        (v.sortWith( _._3 > _._3 ).take(topK)).map { case (u, v, s) => ((u, v), s) }
      }

    return predictBySimilarity(train, test, topKSimilarities, optimized = false)

  }


  /**
    * Compute the Mean Average Error for the baseline method
    *
    * @param train RDD
    * @param test RDD
    * @param similarities cosine similarities between user pairs
    * @param topK Top N similarities taken into account
    * @param predictor function that uses train and test to predict ratings
    * 
    * @return the MAE using the selected predictor
    */
  def maeByKnnPredictor(
    train: RDD[Rating],
    test: RDD[Rating],
    similarities: IndexedSeq[((Int, Int), Double)],
    topK: Int,
    predictor: (RDD[Rating], RDD[(Int, Int)], IndexedSeq[((Int, Int), Double)], Int) => RDD[Rating]
  ): RDD[Double] = {

    val globalAverage = train.averageRating

    val predictions = predictor(train, test.map(r => (r.user, r.item)), similarities, topK)

    val predictionErrors = test.map(r => ((r.user, r.item), r.rating))
      .leftOuterJoin(predictions.map(p => ((p.user, p.item), p.rating)))
      .map { case ((u, i), (r, p)) => scala.math.abs(p.getOrElse(globalAverage) - r) }

    // Verify that predictions and test RDDs are the same size
    // assert(predictionErrors.count == test.count,
    //        s"RDD sizes do not match when computing MAE: ${predictionErrors.count} vs. ${test.count}")
    
    return predictionErrors

  }

  val similarities = cosineSimilarities(train, optimized = false)

  // val maeForKs = List(10, 30, 50, 100, 200, 300, 400, 800, 943)
  val maeForKs = List(943, 800, 400, 300, 200, 100, 50, 30, 10)
    .map(k => (k, maeByKnnPredictor(train, test, similarities, k, cosineKnnPredictor).mean))

  val baseline = 0.7669

  val lowestKWithBetterMaeThanBaseline = maeForKs 
    .filter(_._2 < baseline)
    .sortWith( _._1 < _._1)
    .take(1)
    .toList.headOption
    .getOrElse((-100, 5.0))

  val maeByK = maeForKs.toMap

  def minNumberOfBytesForK(k : Int) = 943 * 8 * k

  def bytesPerGB(ramGB : Int) : Long = ramGB.toLong * 1073741824

  def maxUsersInRAM(ramGB : Int, k : Int) : Long = bytesPerGB(ramGB) / (8 * 3 * k)

  // ################################################################

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> maeByK.get(10), // Datatype of answer: Double
            "MaeForK=30" -> maeByK.get(30), // Datatype of answer: Double
            "MaeForK=50" -> maeByK.get(50), // Datatype of answer: Double
            "MaeForK=100" -> maeByK.get(100), // Datatype of answer: Double
            "MaeForK=200" -> maeByK.get(200), // Datatype of answer: Double
            "MaeForK=300" -> maeByK.get(300), // Datatype of answer: Double
            "MaeForK=400" -> maeByK.get(400), // Datatype of answer: Double
            "MaeForK=800" -> maeByK.get(800), // Datatype of answer: Double
            "MaeForK=943" -> maeByK.get(943), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> lowestKWithBetterMaeThanBaseline._1, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> (lowestKWithBetterMaeThanBaseline._2 -  baseline) // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> minNumberOfBytesForK(10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> minNumberOfBytesForK(30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> minNumberOfBytesForK(50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> minNumberOfBytesForK(100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> minNumberOfBytesForK(200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> minNumberOfBytesForK(300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> minNumberOfBytesForK(400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> minNumberOfBytesForK(800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> minNumberOfBytesForK(943) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> bytesPerGB(16), // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> maxUsersInRAM(16, lowestKWithBetterMaeThanBaseline._1) // Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
