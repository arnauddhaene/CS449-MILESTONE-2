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
class RatingFunctions(rdd : RDD[Rating]) {
  
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

  def ratioCloseTo(global : Double, threshold : Double = 0.5) = 
    1.0 * rdd.values.filter(r => (r - global).abs < threshold).count / rdd.values.count

  def allCloseTo(global: Double, threshold: Double = 0.5) =
    (rdd.values.min > global - threshold) && (rdd.values.max < global + threshold)

}

object PairRDDFunctions {
  implicit def addPairRDDFunctions(rdd: RDD[(Int, Double)]) = new PairRDDFunctions(rdd) 
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
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
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
    * Compute rating prediction using the baseline method.
    *
    * @param train: RDD[Rating]
    * @param test: RDD[(Int, Int)]
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def baselinePrediction(train : RDD[Rating], test : RDD[(Int, Int)]) : RDD[Rating] = {

    // Calculate global average rating
    val globalAvg = train.averageRating

    val userAverageRating = train.toUserPair.averageByKey
    
    val normalizedDeviations = train
      .map(r => (r.user, (r.item, r.rating)))
      .join(userAverageRating)
      .map { case (u, ((i, r), ua)) => Rating(u, i, 1.0 * (r - ua) / scale(r, ua).toDouble) }    
    
    val itemGlobalAverageDeviation = normalizedDeviations.toItemPair.averageByKey

    // Verify that normalized deviations are within range and distinct for (user, item) pairs
    // assert(normalizedDeviations.filter(r => (r.rating > 1.0) || (r.rating < -1.0)).count == 0, 
    //        "Normalization not within range.")
    // assert(normalizedDeviations.map(r => (r.user, r.item)).distinct.count == train.count,
    //        "Non unique pairs of (user, item).")

    val predictions = test
      .join(userAverageRating)
      .map { case (u, (i, ua)) => (i, (ua, u)) }
      .leftOuterJoin(itemGlobalAverageDeviation)
      .map { case (i, ((ua, u), ia)) => 
        ia match {
          case None => Rating(u, i, globalAvg)
          case Some(ia) => 
            Rating(u, i, (ua + ia * scale((ua + ia), ua)))
        }
      }

    // Verify that all predictions are in the range [1.0, 5.0]
    // assert(predictions.filter(p => (p.rating < 1.0) || (p.rating > 5.0)).count == 0,
    //        "Some predictions are out of bounds")

    return predictions

  }

  // TODO: documentation
  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  // TODO: documentation
  def normalizedDeviation(rating: Double, itemAverage: Double) = {
    1.0 * (rating - itemAverage) / scale(rating, itemAverage).toDouble
  }

  // TODO: documentation
  def cosineSimilarityDenominator(ratings: Iterable[Double]) = {
    scala.math.sqrt(
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
  def preprocess(train : RDD[Rating]) : RDD[(Int, Iterable[(Int, Double)])] = {
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
    * Create a map of the similarities between all users
    * @note call the resulting map with key (u, v) only when u > v
    *
    * @param processed: Map[Int, Iterable[(Int, Double)]]
    * @return map of user-user pair similarity values
    */
  def userSimilarities(
    processed : Map[Int, Iterable[(Int, Double)]]
  ) : Map[(Int, Int), Double]= {

    val similarities = (1 to 943)
      .flatMap { 
        case (u) => {
          val uItemRatings = processed.get(u).getOrElse(List()).toMap
          
          // create pairs of similarity indexes
          (1 to u - 1).map {
            case (v) => {
              val vItemRatings = processed.get(v).getOrElse(List()).toMap

              // find the intersection of common items
              ((u, v), uItemRatings.keySet.intersect(vItemRatings.keySet).map(k => k -> ( uItemRatings(k), vItemRatings(k) )).toList)
            }
          }
        }
      }
      // filter out similarities for users with no items in common
      .filter { case ((u, v), l) => !l.isEmpty }
      // reduce list of items in intersection into the similary
      .map { 
        case ((u, v), l) => 
          ((u, v), l.map { case (i, (upr, vpr)) => upr * vpr }.reduce(_+_))
      }

    return (similarities.toMap)
  }

  /**
    * Compute rating prediction using the cosine similarity method.
    *
    * @param train RDD[Rating]
    * @param test RDD[(Int, Int)]
    * 
    * @return RDD[Rating] with the predicted rating for each (user, item) pair
    */
  def cosinePredictor(train : RDD[Rating], test : RDD[(Int, Int)]) = {

    val globalAverage = train.averageRating

    val userAverage = train.toUserPair.averageByKey

    val processed = preprocess(train)
        .collect
        .toMap

    val similarities = userSimilarities(processed)

    println(s"CURRENT TEST SET SIZE ${test.count}")

    val predictions = test
      .leftOuterJoin(userAverage)
      .map { case (u, (i, uAvg)) => (i, (u, uAvg.getOrElse(globalAverage))) }      
      .join(train.map(r => (r.item, (r.user, r.rating))).groupByKey())
      .flatMap { 
        case (i, ((u, uAvg), others)) => 
          others.map {
            case (v, r) => {
              val key = if (u > v) (u, v) else (v, u)

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

    val uncheckedItems = test
      .map { case (u, i) => (i, u) }
      .leftOuterJoin(train.toItemPair)
      .map {
        case (i, (u, r)) => {
          val result = r match {
            case Some(r: Double) => -999.0
            case None => globalAverage
          }

          Rating(u, i, result)
        }
      }
      .filter(_.rating > 0.0)

    println(s"CURRENT TEST SET SIZE ${predictions.count}")
    println(s"UNCHECKED ITEMS SIZE ${uncheckedItems.count}")

    val result = predictions.union(uncheckedItems)

    println(s"CURRENT TEST SET SIZE ${result.count}")

    (result)

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
  ) = {

    val predictionErrors = predictor(train, test.map(r => (r.user, r.item)))
      .map(r => ((r.user, r.item), r.rating))
      .join(test.map(p => ((p.user, p.item), p.rating)))
      .map { case ((u, i), (r, p)) => scala.math.abs(p - r) }

    // Verify that predictions and test RDDs are the same size
    assert(predictionErrors.count() == test.count(),
           "RDD sizes do not match when computing baseline MAE.")
    
    (predictionErrors)

  }

  val cosineBasedMae = maeByPredictor(train, test, cosinePredictor)

  // cosinePredictor(train, test.sample(false, 0.1, 12345).map(r => (r.user, r.item))).take(30).foreach(println)

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
            "CosineBasedMae" -> cosineBasedMae.mean, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> 0.0 // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> 0.0, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> 0.0 // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> 0 // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> 0.0,  // Datatype of answer: Double
              "max" -> 0.0, // Datatype of answer: Double
              "average" -> 0.0, // Datatype of answer: Double
              "stddev" -> 0.0 // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> 0.0, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> 0.0 // Datatype of answer: Double
          )
         )
        json = Serialization.writePretty(answers)
      }

      // println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
