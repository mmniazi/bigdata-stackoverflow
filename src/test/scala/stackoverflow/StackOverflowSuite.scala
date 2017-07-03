package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import stackoverflow.StackOverflow.{groupedPostings, rawPostings, scoredPostings, vectorPostings}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  lazy val sc: SparkContext = new SparkContext(conf)
  val lines: RDD[String] =
    sc.textFile("../../src/test/resources/stackoverflow/stackoverflow.csv")
  val raw: RDD[Posting] = rawPostings(lines)
  val grouped: RDD[(Int, Iterable[(Posting, Posting)])] = groupedPostings(raw)
  val scored: RDD[(Posting, Int)] = scoredPostings(grouped)
  val vectors: RDD[(Int, Int)] = vectorPostings(scored)

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("grouped posting") {
    val javaPostings = grouped.lookup(5257894).head
    assert(javaPostings.size == 3)

    val scoreSum = javaPostings.map(_._2.score).sum
    assert(scoreSum == 5)
  }

  test("scored posting") {
    val maxScore = scored.map(_._2).max()
    assert(maxScore == 4)
    val questions = scored.count()
    assert(questions == 4)
  }

  test("vector posting") {
    val java = vectors.lookup(1 * StackOverflow.langSpread)
    assert(java.sum == 4)
  }

  test("kmeans") {

  }
}
