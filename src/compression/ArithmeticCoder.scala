package compression

import scala.annotation.tailrec
import collection.mutable
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * User: cwmcen
 * Date: 5/13/13
 * Time: 6:37 PM
 */
/**
 * Codes and decodes a sequence of symbols via arithmetic coding using the associated model.
 *
 * @param model   the ArithmeticCoderModel[T] that determines the symbol probabilities
 * @tparam T      the type of symbols encoded
 */
case class ArithmeticCoder[@specialized(Int, Char, Double) T](model: ArithmeticCoderModel[T]) {
  /** General implementation notes **/
  // The general strategy is to progressively identify smaller and smaller intervals that correspond to the full
  // sequence of symbols.  This introduces two complications:
  //  * Typically for a sequence longer than a few symbols, the encoding interval will be so small that rounding effects
  //    due to finite precision can interfere.
  //  * It is possible for the lower and upper bounds of an interval to converge without sharing a common prefix e.g.
  //    0111... and 1000 can both converge to 1/2.  This is an underflow condition.
  //
  // The first is dealt with by using a rounding scheme.  Each of the decimal probabilities are converted to binary
  // numerators in base 14 with appropriate rounding applied.  Each of the interval boundaries are base 16 numbers
  // stored as base 32 ints.  This ensures that any manipulation on the boundaries via the probabilities will result in
  // rounding on bits that aren't kept.
  //
  // The later problem is solved by preemptively identifying situations where underflow might occur and shifting the
  // boundaries hoping that in the shifted coordinates this does not occur.  This is done according to the formula
  //    c^* = 2c - 1/2
  // which can be applied successively.  This has the effect of removing the term of order 1/4. Going back to c from
  // c^*,
  //    c_i = c^*_{i - 1} for i >= 3
  // The first two terms depend on c_1^*.  If c_1^* = 0, c_1 = 0 and c_2 = 1.  Otherwise if c_1^* = 1, then c_1 = 1
  // and c_2 = 0.
  private val Precision = ArithmeticCoder.Precision
  private val MaxValue = (1 << Precision) - 1

  private val LeadBitProjector = 1 << (Precision - 1)
  private val SecondBitProjector = 1 << (Precision - 2)
  private val RemainderProjector = ~LeadBitProjector

  def decode(bits: Iterator[Byte])(complete: IndexedSeq[T] => Boolean): Seq[T] = {
    // The stateful object that handles the decoding process
    val state = new DecoderState(bits.next())

    // Determine the contextual state
    val context = collection.mutable.Queue[T]()
    var cumulativeProbabilities = model.cumulativeProbabilities(context)

    // Decode each symbol individually, until the expected number of symbols have been decoded
    val symbols = collection.mutable.ArrayBuffer[T]()
    while (!complete(symbols)) {
      val cumulativeProbability = cumulativeProbabilities.last

      // The probability bounds based on the current decoded bits
      val minProbability = state.minProbability(cumulativeProbability)
      val maxProbability = state.maxProbability(cumulativeProbability)

      // Identify the symbol with the encoded probability
      identifySymbol(minProbability, maxProbability, cumulativeProbabilities) match {
        case Some(symbolIndex) => {
          val symbol = state.decodeSymbol(symbolIndex, context, cumulativeProbabilities)
          symbols.append(symbol)

          // Update the context
          if (!complete(symbols)) {
            context.enqueue(symbol)
            if (context.length > model.contextLength) context.dequeue()
            cumulativeProbabilities = model.cumulativeProbabilities(context)
          }
        }
        case None => state.incorporateBit(bits.next())
      }
    }

    symbols.toList
  }

  /**
   * Decodes the bits until `length` symbol `T` have been decoded.
   *
   * @param bits      the Iterator[Byte] of bits to decode
   * @param length    the Int number of symbols to decode
   */
  def decode(bits: Iterator[Byte], length: Int): Seq[T] = decode(bits)(_.length == length)

  /**
   * Decodes the bits until the specified symbol `T` has been encountered.
   *
   * @param bits      the Iterator[Byte] of bits to decode
   * @param symbol    the T symbol to stop decoding at
   */
  def decodeUntil(bits: Iterator[Byte], symbol: T): Seq[T] = {
    val decodedSymbols = decode(bits) { symbols =>
      symbols.nonEmpty && symbols.last == symbol
    }
    decodedSymbols.init
  }

  def encode(symbols: Seq[T]): Seq[Byte] = {
    val state = new EncoderState

    // Iterate through each of the symbols progressively reducing the interval that the final point can be chosen in
    // The context required by the model determines how many symbols to examine prior to the symbol to be encoded
    val bits = new java.util.ArrayList[Byte]()

    // Use a stack to keep track of the current state, as the encoder iterates through the list
    val context = collection.mutable.Queue[T]()

    // Encode the initial context
    val remainingSymbols = encodeSymbolContext(symbols, model.contextLength, state, context, bits)

    // Slide through the symbol sequence using the context to encode the next symbol
    encodeSymbolList(remainingSymbols, state, context, bits)

    // Pick the point in the final interval
    val pointBits = pickPoint(state.start, state.stop, state.underflowCount)
    bits.addAll(pointBits.asJavaCollection)

    bits.asScala
  }

  @tailrec
  private def encodeSymbolContext(symbols: Seq[T],
                                  remaining: Int,
                                  state: EncoderState,
                                  context: mutable.Queue[T],
                                  bits: java.util.ArrayList[Byte]): Seq[T] = {
    if (remaining > 0) {
      val symbol = symbols.head
      state.encodeSymbol(symbol, context, bits)

      // Update the context
      context.enqueue(symbol)
      encodeSymbolContext(symbols.tail, remaining - 1, state, context, bits)
    } else symbols
  }

  @tailrec
  private def encodeSymbolList(symbols: Seq[T],
                               state: EncoderState,
                               context: mutable.Queue[T],
                               bits: java.util.ArrayList[Byte]): Unit = {
    if (symbols.nonEmpty) {
      val symbol = symbols.head
      state.encodeSymbol(symbol, context, bits)

      // Update the context
      context.enqueue(symbol)
      context.dequeue()

      encodeSymbolList(symbols.tail, state, context, bits)
    }
  }

  /**
   * Returns the `Option[Int]` index for the symbol matching the provided probability range.  If a unique match is not
   * found then `None` is returned.
   *
   * @param minProbability              the Int minimum probability that could be encoded by the encountered bits
   * @param maxProbability              the Int maximum probability that could be encoded by the encountered bits
   * @param cumulativeProbabilities     the IndexedSeq[Int] with the cumulative probabilities for the current state
   */
  private def identifySymbol(minProbability: Int,
                             maxProbability: Int,
                             cumulativeProbabilities: IndexedSeq[Int]): Option[Int] = {
    def binarySearch(probability: Int): Int = {
      var first = 0
      var last = cumulativeProbabilities.length - 2
      var middle = last / 2

      // binary search
      var processing = true
      while (last >= first && processing) {
        if (probability < cumulativeProbabilities(middle)) {
          // lower bound is higher than probability
          last = middle - 1
          middle = first + ((last - first) / 2)
        } else if (probability >= cumulativeProbabilities(middle + 1)) {
          // upper bound is lower than probability
          first = middle + 1
          middle = first + ((last - first) / 2)
        } else processing = false
      }

      middle
    }

    // Get the bounds and check to see if they unambiguously agree on a single symbol
    val minIndex = binarySearch(minProbability)
    val maxIndex = binarySearch(maxProbability)
    if (minIndex == maxIndex) Some(minIndex) else None
  }

  /**
   * Returns a `Seq[Byte]` encoding a point in the provided interval with a minimal number of bits needed to
   * unambiguously decode this.
   *
   * @param intervalStart         the Int start of the interval
   * @param intervalStop          the Int stop of the interval
   * @param underflowShiftCount   the Int number of underflow shifts that have occurred to get the start and stop
   *                              boundaries
   */
  private def pickPoint(intervalStart: Int, intervalStop: Int, underflowShiftCount: Int): Seq[Byte] = {
    // Check for an exceptional case where the interval is already completely determined and no further bits are needed
    if (intervalStart == 0 && intervalStop == MaxValue && underflowShiftCount == 0) Nil
    else {
      // Choose the seed bit to base the selection process off of, which is done so that the greatest portion of the
      // interval is on the same side as the chosen bit
      val leadBit = if (intervalStart > LeadBitProjector ||
        intervalStop - LeadBitProjector > LeadBitProjector - intervalStart) 1
      else 0

      // Add bits until the interval about the point is unambiguously in the interval
      val encodedPointBits = collection.mutable.ArrayBuffer(leadBit)
      var pickingPoint = true
      while (pickingPoint) {
        val encodedPointPrefix = encodedPointBits.foldLeft(0) { (sum, bit) => (sum << 1) | bit }
        val maxSuffix = (1 << Precision - encodedPointBits.length) - 1

        // When current bits are encountered, these are the possible bounds the decoder will see
        val encodedMinPoint = encodedPointPrefix << Precision - encodedPointBits.length
        val encodedMaxPoint = encodedMinPoint | maxSuffix

        if (encodedMinPoint < intervalStart) {
          // Increase the min by adding a 1
          encodedPointBits.append(1)
        } else if (encodedMaxPoint > intervalStop) {
          // Make the max smaller by adding another zero
          encodedPointBits.append(0)
        } else pickingPoint = false
      }

      // Reconcile the selected point with any underflow shifts
      val underflowBits = 1.to(underflowShiftCount).map { i => leadBit ^ 1 }
      val unshiftedBits = (leadBit +: underflowBits) ++ encodedPointBits.tail

      unshiftedBits.map(_.toByte)
    }
  }

  abstract class CoderState {
    // These set the boundaries of the last identified interval
    protected var lastIntervalStart = 0
    protected var lastIntervalStop = (1 << Precision) - 1

    def range: Int = stop - start + 1
    def start: Int = lastIntervalStart
    def stop: Int = lastIntervalStop

    /**
     * Updates the interval reflecting the sub-interval corresponding to the provided symbol.
     *
     * @param symbolIndex               the Int index of the symbol in the cumulativeProbabilities
     * @param cumulativeProbabilities   an IndexedSeq[Int] with the cumulative binary probability numerators for all
     *                                  of the symbol possibilities
     */
    def adjustInterval(symbolIndex: Int, cumulativeProbabilities: IndexedSeq[Int]) {
      val cumulativeProbability = cumulativeProbabilities.last

      // new start = old start + rescaled new start
      val rescaledStart = cumulativeProbabilities(symbolIndex) * range / cumulativeProbability
      val newStart = lastIntervalStart + rescaledStart

      // new stop = old start + rescaled stop - 1
      val rescaledStop = cumulativeProbabilities(symbolIndex + 1) * range / cumulativeProbability
      val newStop = lastIntervalStart + rescaledStop - 1

      // Update the interval boundaries
      lastIntervalStart = newStart
      lastIntervalStop = newStop
    }

    /**
     * Returns true if both the start and the stop share the same leading bit.
     */
    protected def leadingBitsMatch: Boolean = {
      (lastIntervalStart & LeadBitProjector) == (lastIntervalStop & LeadBitProjector)
    }

    /**
     * Returns true if the second bits of both the start and stop don't match, which suggests that a possible underflow
     * condition could be occurring.  It is assumed that both of the leading bits don't match, since otherwise that
     * would mean they could be removed.
     */
    protected def potentialUnderflow: Boolean = {
      ((~lastIntervalStop & lastIntervalStart) & SecondBitProjector) != 0
    }
  }

  class DecoderState(leadBit: Byte) extends CoderState {
    protected var encodedPrefix = leadBit << (Precision - 1)
    protected var encodedPrefixLength = 1

    /**
     * Returns the `Int` minimum probability value that can occur given the current prefix.  This is achieved by
     * assuming that all remaining bits are 0.
     *
     * @param cumulativeProbability   the Int cumulative probability for this state
     */
    def minProbability(cumulativeProbability: Int): Int = {
      val minEncodedValue = encodedPrefix
      ((minEncodedValue - start + 1) * cumulativeProbability - 1) / range
    }

    /**
     * Returns the `Int` maximum probability value that can occur given the current prefix.  This is achieved by
     * assuming that all remaining bits are 1.
     *
     * @param cumulativeProbability   the Int cumulative probability for this state
     */
    def maxProbability(cumulativeProbability: Int): Int = {
      val maxSuffix = (1 << Precision - encodedPrefixLength) - 1
      val maxEncodedValue = encodedPrefix | maxSuffix
      ((maxEncodedValue - start + 1) * cumulativeProbability - 1) / range
    }

    /**
     * Appends the provided bit to the encodedPrefix.
     *
     * @param bit   the Byte bit to incorporate
     */
    def incorporateBit(bit: Byte) {
      val positionedBit = bit << Precision - encodedPrefixLength - 1
      encodedPrefix |= positionedBit
      encodedPrefixLength += 1
    }

    /**
     * Returns the `T` symbol at the index under the given context.  The interval is adjusted to reflect the decoding.
     *
     * @param index                       the Int index of the symbol to decode
     * @param context                     the Seq[T] context of the symbol
     * @param cumulativeProbabilities     the IndexedSeq[Int] of cumulative probabilities under the context
     */
    def decodeSymbol(index: Int, context: Seq[T], cumulativeProbabilities: IndexedSeq[Int]): T = {
      // Find the corresponding symbol
      val symbol = model.symbolAt(index)

      // Update the interval and remove unneeded bits
      adjustInterval(index, cumulativeProbabilities)
      model.update(symbol, context)
      decodeBits()

      symbol
    }

    /**
     * Removes exhausted bits from the interval boundaries and prefix.
     */
    private def decodeBits() {
      var processing = true
      while (processing) {
        if (leadingBitsMatch) {
          // Do nothing
        } else if (potentialUnderflow) {
          // Handle possible underflow
          lastIntervalStart &= ~(LeadBitProjector | SecondBitProjector)
          lastIntervalStop |= SecondBitProjector
          encodedPrefix ^= SecondBitProjector
        } else processing = false

        if (processing) {
          // Remove the leading bit
          lastIntervalStart = (lastIntervalStart & RemainderProjector) << 1
          lastIntervalStop = (lastIntervalStop & RemainderProjector) << 1 | 1
          encodedPrefix = (encodedPrefix & RemainderProjector) << 1
          encodedPrefixLength -= 1
        }
      }
    }
  }

  class EncoderState extends CoderState {
    // The number of times an underflow shift has been performed
    protected var underflowShiftCount = 0

    def underflowCount: Int = underflowShiftCount

    /**
     * Updates the interval to reflect the encoding of the symbol and associated context and appends the bits that have
     * been completely determined.
     *
     * @param symbol    the T symbol to encode
     * @param context   the Seq[T] context preceding the symbol
     */
    def encodeSymbol(symbol: T, context: Seq[T], bits: java.util.ArrayList[Byte]): Unit = {
      val cumulativeProbabilities = model.cumulativeProbabilities(context)
      val symbolIndex = model.indexOf(symbol)
      adjustInterval(symbolIndex, cumulativeProbabilities)

      // Update the model
      model.update(symbol, context)

      encodeBits(bits)
    }

    /**
     * Appends the bits that have been completely determined by the interval boundaries.  The
     * boundaries are correspondingly updated so that there is no redundant information between them.
     */
    private def encodeBits(bits: java.util.ArrayList[Byte]): Unit = {
      var processing = true
      while (processing) {
        if (leadingBitsMatch) {
          val leadingBit = (lastIntervalStart & LeadBitProjector) >> (Precision - 1)
          bits.add(leadingBit.toByte)

          // Write out any bits in the underflow
          while (underflowShiftCount > 0) {
            val underflowBit = (lastIntervalStop ^ LeadBitProjector) >> (Precision - 1)
            bits.add(underflowBit.toByte)

            // Decrement the underflow
            underflowShiftCount -= 1
          }
        } else if (potentialUnderflow) {
          // Possible underflow situation
          underflowShiftCount += 1

          lastIntervalStart &= ~(LeadBitProjector | SecondBitProjector)
          lastIntervalStop |= SecondBitProjector
        } else processing = false

        if (processing) {
          // Remove the leading bits
          lastIntervalStart = (lastIntervalStart & RemainderProjector) << 1
          lastIntervalStop = (lastIntervalStop & RemainderProjector) << 1 | 1
        }
      }
    }
  }
}

object ArithmeticCoder {
  val Precision = 16

  def build[T](contextLength: Int, symbols: Seq[T]): ArithmeticCoder[T] = {
    apply(ArithmeticCoderModel(contextLength, symbols))
  }
}
