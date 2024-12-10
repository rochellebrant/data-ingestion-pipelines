println("Next, I'll spawn a notebook")
val a = 2
val result = dbutils.notebook.run("./Addition", timeoutSeconds = 60, arguments = Map("a" -> a.toString,"b" -> "4"))
println(result)
