   // VALUES are immutable constants.
   val hello: String = "Hola!"

   // VARIABLES are mutable
   var helloThere: String = hello
   helloThere = hello + " There!"
   println(helloThere)

   val immutableHelloThere = hello + " There"
   println(immutableHelloThere)

   // Data Types

   val numberOne: Int = 1
   val truth: Boolean = true
   val letterA: Char = 'a'
   val pi: Double = 3.14159265
   val piSinglePrecision: Float = 3.14159265f
   val bigNumber: Long = 123456789
   val smallNumber: Byte = 127

   println("Here is a mess: " + numberOne + truth + letterA + pi + bigNumber)

   // f means "format"
   // $ variable name
   println(f"Pi is about $piSinglePrecision%.3f") //Only 3 digits after comma
   println(f"Zero padding on the left: $numberOne%05d") // five digits to the left of the decimal point

   //Concatenate string
   println(s"I can use the s prefix to use variables like $numberOne $truth $letterA")

   println(s"The s prefix isn't limited to variables; I can include any expression. Like ${1+2}")

   //Define a string
   val theUltimateAnswer: String = "To life, the universe, and everything is 42."
   //Extract the answer, extract info from a string
   //Extract digit from the string: d means number followed by anything else
   val pattern = """.* ([\d]+).*""".r
   val pattern(answerString) = theUltimateAnswer
   val answer = answerString.toInt
   println(answer)

   // Booleans
   val isGreater = 1 > 2
   val isLesser = 1 < 2
   // && logical operator
   val impossible = isGreater & isLesser //Bitwise AND
   val anotherWay = isGreater || isLesser

   val picard: String = "Picard"
   val bestCaptain: String = "Picard"
   val isBest: Boolean = picard == bestCaptain
   println(isBest)

   // EXERCISE
   // Write some code that takes the value of pi, doubles it, and then prints it within a string with
   // three decimal places of precision to the right.
