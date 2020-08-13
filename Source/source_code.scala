import org.apache.spark.graphx.GraphLoader

val graph = GraphLoader.edgeListFile(sc,"/user/ravichaubey43_gmail/followers.txt")

val ranks = graph.pageRank(0.0001).vertices

ranks.collect

val users = sc.textFile("/user/ravichaubey43_gmail/users.txt").map { line =>
     | 
     |   val fields = line.split(",")
     | 
     |   (fields(0).toLong, fields(1))
     | 
     | }
     
val ranksByUsername = users.join(ranks).map {
     | 
     |   case (id, (username, rank)) => (username, rank)
     | 
     | }
     
println(ranksByUsername.collect().mkString("\n"))
