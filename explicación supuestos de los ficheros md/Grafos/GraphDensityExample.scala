import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object GraphDensityExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Graph Density Example").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      // Crear un grafo de ejemplo
      val vertices: RDD[(VertexId, String)] = sc.parallelize(Array(
        (1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"), (5L, "E")
      ))

      val edges: RDD[Edge[Int]] = sc.parallelize(Array(
        Edge(1L, 2L, 1), Edge(1L, 3L, 1), Edge(2L, 3L, 1),
        Edge(2L, 4L, 1), Edge(3L, 5L, 1), Edge(4L, 5L, 1)
      ))

      val graph: Graph[String, Int] = Graph(vertices, edges)

      // Calcular la densidad
      val numVertices = graph.vertices.count()
      val numEdges = graph.edges.count()
      val maxPossibleEdges = numVertices * (numVertices - 1) / 2 // Para un grafo no dirigido
      val density = numEdges.toDouble / maxPossibleEdges

      println(s"Número de vértices: $numVertices")
      println(s"Número de aristas: $numEdges")
      println(s"Máximo número posible de aristas: $maxPossibleEdges")
      println(f"Densidad del grafo: $density%.4f")

    } catch {
      case e: Exception =>
        println(s"Ocurrió un error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}