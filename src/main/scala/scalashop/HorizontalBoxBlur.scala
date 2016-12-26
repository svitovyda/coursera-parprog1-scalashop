package scalashop

import org.scalameter._
import common._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object HorizontalBoxBlurRunner {

  val standardConfig: MeasureBuilder[Unit, Double] = config(
    Key.exec.minWarmupRuns -> 2,
    Key.exec.maxWarmupRuns -> 4,
    Key.exec.benchRuns -> 5,
    Key.verbose -> true
  ) withWarmer new Warmer.Default

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqTime = standardConfig measure {
      HorizontalBoxBlur.blur(src, dst, 0, height, radius)
    }

    val numTasks = 32
    val parTime = standardConfig measure {
      HorizontalBoxBlur.parBlur(src, dst, numTasks, radius)
    }

    val parNonCorners = standardConfig measure {
      HorizontalBoxBlur.parBlur(src, dst, numTasks, radius)
    }

    val parFutures = standardConfig measure {
      HorizontalBoxBlur.parFutures(src, dst, numTasks, radius)
    }

    val parFuturesNoCorners = standardConfig measure {
      HorizontalBoxBlur.parFuturesNonCorner(src, dst, numTasks, radius)
    }
    println(s"sequential blur time: $seqTime ms")

    println(s"non-corner time: $parNonCorners ms")
    println(s"futures time: $parFutures ms")
    println(s"futures no c time: $parFuturesNoCorners ms")
    println(s"fork/join blur time: $parTime ms")

    println(s"speedup: ${seqTime / parTime}")
    println(s"speedup non-corner: ${seqTime / parNonCorners}, ${parTime / parNonCorners}")
    println(s"speedup futures: ${seqTime / parFutures}, ${parTime / parFutures}")
    println(s"speedup futures: ${seqTime / parFuturesNoCorners}, ${parTime / parFuturesNoCorners}")
  }
}

/** A simple, trivially parallelizable computation. */
object HorizontalBoxBlur {

  /** Blurs the rows of the source image in parallel using `numTasks` tasks.
    *
    * Parallelization is done by stripping the source image `src` into
    * `numTasks` separate strips, where each strip is composed of some number of
    * rows.
    */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val rowsNum: Int = Math.max(src.height / numTasks, 1)

    val tasks = (Range(0, src.height) by rowsNum) map { start =>
      task {
        blur(src, dst, start, start + rowsNum, radius)
      }
    }

    tasks.foreach(_.join)
  }

  def parNonCorners(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val rowsNum: Int = Math.max(src.height / numTasks, 1)

    val tasks = (Range(0, src.height) by rowsNum) map { start =>
      task {
        blurNoCorners(src, dst, start, start + rowsNum, radius)
      }
    }

    tasks.foreach(_.join)
  }

  def parFutures(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val rowsNum: Int = Math.max(src.height / numTasks, 1)

    val tasks = (Range(0, src.height) by rowsNum) map { start =>
      Future {
        blur(src, dst, start, start + rowsNum, radius)
      }
    }

    Await.ready(Future.sequence(tasks), 2.minutes)
  }

  def parFuturesNonCorner(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val rowsNum: Int = Math.max(src.height / numTasks, 1)

    val tasks = (Range(0, src.height) by rowsNum) map { start =>
      Future { blurNoCorners(src, dst, start, start + rowsNum, radius) }
    }

    Await.ready(Future.sequence(tasks), 1.minute)
  }

  /** Blurs the rows of the source image `src` into the destination image `dst`,
    * starting with `from` and ending with `end` (non-inclusive).
    *
    * Within each row, `blur` traverses the pixels by going from left to right.
    */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    val yy = Math.min(end, src.height)
    for {
      x <- 0 until src.width
      y <- from until yy
    } {
      dst.update(x, y, boxBlurKernel(src, x, y, radius))
    }
  }

  def blurNoCorners(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    val yy = Math.min(end, src.height - radius)
    for {
      x <- radius until src.width
      y <- from until yy
    } {
      dst.update(x, y, boxBlurKernelNonCorner(src, x, y, radius))
    }
  }
}
