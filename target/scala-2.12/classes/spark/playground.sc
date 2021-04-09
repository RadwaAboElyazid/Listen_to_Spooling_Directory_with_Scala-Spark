
val t = Map("a" -> 1, "b" -> 2, "c" -> 3)
val myList = List(0,1,2)
myList :+ 5
println(myList)


val seq = t.keys
for(s <- seq) {
  println(t(s))
}


