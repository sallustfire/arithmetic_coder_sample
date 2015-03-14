package compression

import scala.collection.mutable.{HashMap => MutableHashMap, IndexedSeq => MutableIndexedSeq, ArraySeq}

class ArithmeticCoderModel[T](val contextLength: Int, symbols: Seq[T]) {
  private val indexedSymbols = symbols.toIndexedSeq
  private val symbolIndexes = indexedSymbols.zipWithIndex.toMap
  private val cumulativeFrequencies = new MutableHashMap[Int, MutableIndexedSeq[Int]]()
  private val defaultCumulative = 0.to(indexedSymbols.length)

  /**
   * Returns the interval boundaries for the decimal probability numerators.
   *
   * @param context the Seq[T] of context symbols
   */
  def cumulativeProbabilities(context: Seq[T]): IndexedSeq[Int] = {
    if (context.length < contextLength) {
      defaultCumulative
    } else if (context.length == contextLength) {
      cumulativeFrequencies.get(context.hashCode()) match {
        case Some(frequencies) => frequencies
        case None => defaultCumulative
      }
    } else {
      throw new IllegalArgumentException("invalid number of symbols in context")
    }
  }

  def indexOf(symbol: T): Int = symbolIndexes(symbol)
  def symbolAt(index: Int): T = indexedSymbols(index)

  /**
   * Given the provided context, updates the probabilities for observing the provided symbol.
   *
   * @param context the Seq[T] of context symbols
   */
  def update(symbol: T, context: Seq[T]): Unit = {
    if (context.length < contextLength) {
      // Do nothing
    } else if (context.length == contextLength) {
      val contextCode = context.hashCode()
      cumulativeFrequencies.get(contextCode) match {
        case Some(frequencies) => {
          (indexOf(symbol) + 1).to(frequencies.size - 1).foreach { index =>
            frequencies.update(index, frequencies(index) + 1)
          }

          // Check if the frequencies have gone out of range
          if (frequencies.last > Math.pow(2, ArithmeticCoder.Precision - 2)) scaleFrequencies(frequencies)
        }
        case None => cumulativeFrequencies.put(contextCode, ArraySeq(defaultCumulative: _*))
      }
    } else {
      throw new IllegalArgumentException("invalid number of symbols in context")
    }
  }

  private def scaleFrequencies(frequencies: MutableIndexedSeq[Int]): Unit = {
    for { (frequency, index) <- frequencies.zipWithIndex } {
      val scaledFrequency = Math.ceil(frequency.toDouble / 2)
      frequencies.update(index, scaledFrequency.toInt)
    }
  }
}

object ArithmeticCoderModel {
  def apply[T](contextLength: Int, symbols: Seq[T]): ArithmeticCoderModel[T] = {
    new ArithmeticCoderModel(contextLength, symbols)
  }
}