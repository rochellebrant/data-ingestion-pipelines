val a = dbutils.widgets.get("a").toInt
val b = dbutils.widgets.get("b").toInt
val c = a + b
dbutils.notebook.exit(c.toString)
