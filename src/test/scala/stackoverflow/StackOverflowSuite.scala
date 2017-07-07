package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import stackoverflow.StackOverflow._

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  lazy val sc: SparkContext = new SparkContext(conf)
  val lines: RDD[String] =
    sc.textFile("../../src/test/resources/stackoverflow/stackoverflow.csv")

  lazy val testObject = new StackOverflow {
    override val langs = List("Java", "Python", "C#", "C++")

    override def langSpread = 50000

    override def kmeansKernels = 4

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  val raw: RDD[Posting] = testObject.rawPostings(lines)
  val grouped: RDD[(Int, Iterable[(Posting, Posting)])] = testObject.groupedPostings(raw)
  val scored: RDD[(Posting, Int)] = testObject.scoredPostings(grouped)
  val vectors: RDD[(Int, Int)] = testObject.vectorPostings(scored)
  val samples: Array[(Int, Int)] = testObject.sampleVectors(vectors)
  val means: Array[(Int, Int)] = testObject.kmeans(samples, vectors)

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

    val size = grouped.count()
    assert(size == 5)
  }

  test("scored posting") {
    val maxScore = scored.map(_._2).max()
    assert(maxScore == 4)
    val questions = scored.count()
    assert(questions == 5)
  }

  test("vector posting") {
    val java = vectors.lookup(0)
    assert(java.sum == 4)
    val size = vectors.count()
    assert(size == 5)
  }

  test("kmeans") {
    val java = means.find({ case (index, score) =>
      index == 0
    }).get

    assert(java._2 == 2)
  }
}
