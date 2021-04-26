package similarity

import similarity.RatingFunctions._
import similarity.PairRDDFunctions._

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

case class Rating(user: Int, item: Int, rating: Double)

// Extension of RDD[Rating] with custom operators
class RatingFunctions(rdd: RDD[Rating]) {
  
  def averageRating = rdd.map(_.rating).mean

  def toUserItemPair = rdd.map(r => (r.user, r.item))
  def toUserPair = rdd.map(r => (r.user, r.rating))
  def toItemPair = rdd.map(r => (r.item, r.rating))

}

object RatingFunctions {
  implicit def addRatingFunctions(rdd: RDD[Rating]) = new RatingFunctions(rdd) 
}

// Extension of RDD[(Int, Double)] with custom operators
class PairRDDFunctions(rdd : RDD[(Int, Double)]) {

  def values = rdd.map(_._2)

  def averageByKey = rdd
    .aggregateByKey((0.0, 0))(
      (k, v) => (k._1 + v, k._2 + 1),
      (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    .mapValues(sum => 1.0 * sum._1 / sum._2.toDouble)

  def ratioCloseTo(global: Double, threshold: Double = 0.5) = 
    1.0 * rdd.values.filter(r => (r - global).abs < threshold).count / rdd.values.count

  def allCloseTo(global: Double, threshold: Double = 0.5) =
    (rdd.values.min > global - threshold) && (rdd.values.max < global + threshold)

}

object PairRDDFunctions {
  implicit def addPairRDDFunctions(rdd: RDD[(Int, Double)]) = new PairRDDFunctions(rdd) 
}

// modular way of timing prediction compute
object PredictionTimer {

  /**
    * Time similarity computation and total prediction time
    *
    * @param predictor
    * @param similarityComputor
    * @param train
    * @param test
    * 
    * @return List[Double], List[Double]: times taken for computation
    */
  def time(
    predictor: (RDD[Rating], RDD[(Int, Int)], Map[(Int, Int), Double], Boolean) => RDD[Rating], 
    similarityComputor: (RDD[Rating], Boolean) => IndexedSeq[((Int, Int), Double)], 
    train: RDD[Rating], test: RDD[Rating]
  ): (Double, Double) = {
    
    val start = System.nanoTime()

    val similarities = similarityComputor(train, true).toMap
    
    val timeSimilarities = (System.nanoTime() - start) / 1e3d
    
    val predictions = predictor(train, test.toUserItemPair, similarities, true)
      // reduce in order to collect RDD
      .map(_.rating).reduce(_ + _)
    
    val timeTotal = (System.nanoTime() - start) / 1e3d

    return (timeSimilarities, timeTotal)

  }

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
    * Computes x scaled by the user average rating.
    *
    * @param x 
    * @param userAvg
    * 
    * @return x scaled by userAvg
    */
  def scale(x : Double, userAvg : Double) : Double = {
    x match {
      case _ if x > userAvg => (5.0 - userAvg)
      case _ if x < userAvg => (userAvg - 1.0)
      case userAvg => 1.0
    }
  }

  /**
    * Average of an Iterable of a Numeric Type
    *
    * @param ts: iterable
    * 
    * @return average
    */
  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  /**
    * Variance of an Iterable of a Numeric Type
    *
    * @param xs: Iterable
    * 
    * @return variance
    */
  def variance[T](xs: Iterable[T])(implicit num: Numeric[T]) = {
    val avg = average(xs)
    xs.map(num.toDouble(_)).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  /**
    * Standard deviation of an Iterable of a Numeric Type
    *
    * @param xs: Iterable
    * 
    * @return standard deviation
    */
  def stdev[T](xs: Iterable[T])(implicit num: Numeric[T]) = math.sqrt(variance(xs))


  /**
    * Normalized deviation of an item's rating following Miletone 1 definition
    *
    * @param rating
    * @param itemAverage
    * 
    * @return Double
    */
  def normalizedDeviation(rating: Double, itemAverage: Double): Double = {
    return 1.0 * (rating - itemAverage) / scale(rating, itemAverage).toDouble
  }

  /**
    * Cosine similarity denominator from Eq. 4 Milestone 2
    *
    * @param ratings
    * @return
    */
  def cosineSimilarityDenominator(ratings: Iterable[Double]): Double = {
    return scala.math.sqrt(
      ratings
        .map(r => scala.math.pow(normalizedDeviation(r, average(ratings)), 2))
        .sum
    )
  }

  /**
    * Preprocess ratings following Equation 4
    *
    * @param train: RDD[Rating]
    * @return RDD[(Int, Iterable[(Int, Double)])] of preprocessed ratings
    */
  def preprocess(train: RDD[Rating]): RDD[(Int, Iterable[(Int, Double)])] = {
    return train
      // key on user id
      .map(r => (r.user, (r.item, r.rating)))
      .groupByKey() 
      // split List[Tuple[itemId, rating]] -> Tuple[List[itemId], List[rating]]
      .map { case (u, l) => (u, l.unzip)}
      // calculate cosine similarity denominator for each user
      .map { 
        case (u, (items, ratings)) => 
          (u, (items, ratings, cosineSimilarityDenominator(ratings))) 
      }
      // compute the processed rating (Eq. 4) for each user's ratings
      .map { 
        case (u, (items, ratings, csd)) => 
          (u, (items, ratings.map(normalizedDeviation(_, average(ratings)) / csd)))
      }
      // zip each item with its processed rating
      .map {
        case (u, (items, pRatings)) => (u, (items zip pRatings))
      }
  }

  /**
    * Jaccard similarity coefficient 
    *
    * @param a: set of items rated by user u
    * @param b: set of items rated by user v
    * 
    * @return Double: intersection over union
    */
  def jaccard(a: Set[Int], b: Set[Int]): Double = {
    // edge case - division by zero
    // this will never occur as we filter out empty `li` in line 46
    // just in case, we check here
    val in = (a intersect b).size.toDouble
    val un = (a union b).size.toDouble

    return if (un == 0.0) (0.0) else (in / un)
  }

  /**
    * Compute Jaccard similarity coefficients for all users in a training set
    *
    * @param train
    */
  def jaccardSimilarities(train: RDD[Rating]) : Map[(Int, Int), Double] = {

    val likedMovies = train.toUserItemPair
      .groupByKey()
      .collect
      .toMap

    val similarities = (1 to 943)
      .flatMap { 
        case (u) => {
          val uMovies = likedMovies.get(u).getOrElse(List()).toSet
          
          // create pairs of similarity indexes
          (1 to u - 1).map {
            case (v) => {
              val vMovies = likedMovies.get(v).getOrElse(List()).toSet

              // compute jaccard coefficient
              ((u, v), jaccard(uMovies, vMovies))
            }
          }
        }
      }

    return (similarities.toMap)

  }

  /**
    * Create a map of the cosine similarities between all users
    * @note call the resulting map with key (u, v) only when u > v if optimized set to true
    *
    * @param processed: Map[Int, Iterable[(Int, Double)]]
    * @param optimized: calculate only a triangular matrix of similarities
    * 
    * @return map of user-user pair similarity values
    */
  def cosineSimilarities(train: RDD[Rating], optimized: Boolean = true): IndexedSeq[((Int, Int), Double)] = {

    val processed = preprocess(train)
      .collect
      .toMap
 
    val nbUsers = processed.size

    val similarities = (1 to nbUsers)
      .flatMap { 
        case (u) => {
          val uItemRatings = processed.get(u).getOrElse(List()).toMap
          
          // optimize calculations of similarities based on KNN or not
          val nbForInternalMap = if (optimized) (u - 1) else (nbUsers)

          // create pairs of similarity indexes
          (1 to nbForInternalMap).map {
            case (v) => {
              val vItemRatings = processed.get(v).getOrElse(List()).toMap

              // find the intersection of common items
              ((u, v), uItemRatings.keySet.intersect(vItemRatings.keySet).map(k => k -> ( uItemRatings(k), vItemRatings(k) )).toList)
            }
          }
          .filter { case ((u, v), l) => u != v }
          // reduce list of items in intersection into the similary
          .map { 
            case ((u, v), l) => 
              ((u, v), if (l.isEmpty) 0.0 else l.map { case (i, (upr, vpr)) => upr * vpr }.reduce(_+_))
          }
        }
      }

    return similarities
  }

  /**
    * Compute the minimum multiplications required for each similarity 
    *
    * @param train
    * 
    * @return Iterable[Int] number of multiplications for each pair of users
    */
  def multiplicationsRequired(train: RDD[Rating]): Iterable[Int] = {

    val likedMovies = train.toUserItemPair
      .groupByKey()
      .collect
      .toMap

    val multiplications = (1 to 943)
      .flatMap { 
        case (u) => {
          val uMovies = likedMovies.get(u).getOrElse(List()).toSet
          
          // create pairs of similarity indexes
          (1 to u - 1).map {
            case (v) => {
              val vMovies = likedMovies.get(v).getOrElse(List()).toSet

              // compute jaccard coefficient
              (uMovies intersect vMovies).size
            }
          }
        }
      }

    // Verify that we are calculating the adequate amount of similarities
    // assert(multiplications.size == 444153)

    return multiplications

  }

  /**
    * Compute predictions based on similarities (Eq 2 and 3)
    *
    * @param train
    * @param test
    * @param similarities
    * @param optimized: fetch u smaller than v for triangular similarities
    */
  def predictBySimilarity(
    train: RDD[Rating], test: RDD[(Int, Int)], 
    similarities: Map[(Int, Int), Double], optimized: Boolean = true
  ): RDD[Rating] = {

    val globalAverage = train.averageRating
    val userAverage = train.toUserPair.averageByKey

    return test
      .leftOuterJoin(userAverage)
      .map { case (u, (i, uAvg)) => (i, (u, uAvg.getOrElse(globalAverage))) }      
      .join(train.map(r => (r.item, (r.user, r.rating))).groupByKey())
      .flatMap { 
        case (i, ((u, uAvg), others)) => 
          others.map {
            case (v, r) => {
              var key = (u, v)
              
              if (optimized) {
                key = if (u > v) (u, v) else (v, u)
              }

              (v, (u, i, uAvg, similarities.get(key).getOrElse(0.0), r))
            }
          }
      }
      .join(userAverage)
      .map { 
        case (v, ((u, i, uAvg, s, r), vAvg)) => 
          ((u, i, uAvg), (s, normalizedDeviation(r, vAvg))) 
      }
      .groupByKey()
      .map { 
        case ((u, i, uAvg), lsd) =>
          val nom = lsd.map { case (s, nd) => s * nd }.reduce(_+_)
          val denom = lsd.map { case (s, _) => scala.math.abs(s) }.reduce(_+_)

          val userSpecWeigSumDev = if (denom == 0.0) (0.0) else (nom / denom.toDouble)

          (Rating(u, i, uAvg + userSpecWeigSumDev * scale((uAvg + userSpecWeigSumDev), uAvg)))
      }
  }

  /**
    * Compute rating prediction using the cosine similarity method.
    *
    * @param train RDD[Rating]
    * @param test RDD[(Int, Int)]
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def cosinePredictor(train : RDD[Rating], test : RDD[(Int, Int)]):  RDD[Rating] = {

    val similarities = cosineSimilarities(train).toMap

    return predictBySimilarity(train, test, similarities)

  }


  /**
    * Compute rating prediction using the jaccard coefficient method.
    *
    * @param train RDD[Rating]
    * @param test RDD[(Int, Int)]
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def jaccardPredictor(train : RDD[Rating], test : RDD[(Int, Int)]): RDD[Rating] = {

    val similarities = jaccardSimilarities(train)

    return predictBySimilarity(train, test, similarities)

  }

  /**
    * Compute the Mean Average Error for the baseline method
    *
    * @param train RDD
    * @param test RDD
    * @param predictor function that uses train and test to predict ratings
    * 
    * @return the MAE using the selected predictor
    */
  def maeByPredictor(
    train : RDD[Rating],
    test : RDD[Rating],
    predictor : (RDD[Rating], RDD[(Int, Int)]) => RDD[Rating]
  ) : RDD[Double] = {

    val globalAverage = train.averageRating

    val predictions = predictor(train, test.map(r => (r.user, r.item)))

    val predictionErrors = test.map(r => ((r.user, r.item), r.rating))
      .leftOuterJoin(predictions.map(p => ((p.user, p.item), p.rating)))
      .map { case ((u, i), (r, p)) => scala.math.abs(p.getOrElse(globalAverage) - r) }

    // Verify that predictions and test RDDs are the same size
    // assert(predictionErrors.count == test.count,
    //        s"RDD sizes do not match when computing MAE: ${predictionErrors.count} vs. ${test.count}")
    
    return predictionErrors

  }

  // As provided in Milestone guidelines
  val baselineMae = 0.7669

  val cosineBasedMae = maeByPredictor(train, test, cosinePredictor).mean
  val jaccardBasedMae = maeByPredictor(train, test, jaccardPredictor).mean

  val multiplications = multiplicationsRequired(train)
  val nonZeroSimilarities = multiplications.filter(_ != 0)

  val timeVectors = (1 to 5).map(iter => {
    println(s"Iteration $iter")
    (PredictionTimer.time(predictBySimilarity, cosineSimilarities, train, test))
  }).unzip

  val timeForSimilarities = timeVectors._1
  val timeForPredictions = timeVectors._2

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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosineBasedMae, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosineBasedMae - baselineMae) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccardBasedMae, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccardBasedMae - cosineBasedMae) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> multiplications.size // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> multiplications.min,  // Datatype of answer: Double
              "max" -> multiplications.max, // Datatype of answer: Double
              "average" -> average(multiplications), // Datatype of answer: Double
              "stddev" -> stdev(multiplications) // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> nonZeroSimilarities.size * 8 // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> timeForPredictions.min,  // Datatype of answer: Double
              "max" -> timeForPredictions.max, // Datatype of answer: Double
              "average" -> average(timeForPredictions), // Datatype of answer: Double
              "stddev" -> stdev(timeForPredictions) // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> timeForSimilarities.min,  // Datatype of answer: Double
              "max" -> timeForSimilarities.max, // Datatype of answer: Double
              "average" -> average(timeForSimilarities), // Datatype of answer: Double
              "stddev" -> stdev(timeForSimilarities) // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> average(timeForSimilarities).toDouble / multiplications.size, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> average(timeForSimilarities).toDouble / average(timeForPredictions) // Datatype of answer: Double
          )
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
