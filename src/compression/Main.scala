package compression

import scala.util.Random

object Main extends App {
  val contextLength = 2
  val symbols = "ACGT".toCharArray

  val random = new Random(1729)

  val symbolBlock = 1.to(10000).foldLeft(Seq[Char]()) { (previous, index) =>
    val nextSymbol: Char = if (random.nextDouble() < 0.5) previous.head
    else symbols(random.nextInt(4))

    previous :+ nextSymbol
  }
  Console.println(s"Encoding ${symbolBlock.size} symbols (${symbolBlock.size * 2} bits)")

  val result = ArithmeticCoder.build(contextLength, symbols).encode(symbolBlock)
  Console.println(s"Compressed bits: ${result.size}")

  val decodedSymbolBlock = ArithmeticCoder.build(contextLength, symbols).decode(result.iterator, symbolBlock.size)
  Console.println(s"Roundtrip is lossless: ${symbolBlock == decodedSymbolBlock}")
}
