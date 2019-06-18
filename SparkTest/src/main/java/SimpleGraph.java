import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Mohib on 4/8/2019.
 */
public class SimpleGraph {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");// run in local mode
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> line = sc.textFile("wordcount2.txt");

        JavaRDD<String> numbers = line.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaRDD<Graph> graphs = numbers.map(new GetGraph());

        Graph graph = graphs.reduce(
                new GetMergedGraph()
        );

        System.out.print(graph);
    }

    static class GetGraph implements Function<String, Graph> {
        public Graph call(String s) {
            Graph graph = new Graph();
            if (Integer.parseInt(s) == 1) {
                //Make first graph and return
                graph.addVertex('a');   //5592
                graph.addVertex('b');
                graph.addVertex('c');

                graph.addEdge('a', 'b');
                graph.addEdge('a', 'c');
            } else if (Integer.parseInt(s) == 2) {
                //Make second graph and return
                graph.addVertex('c');   //5638
                graph.addVertex('d');
                graph.addVertex('e');

                graph.addEdge('c', 'd');
                graph.addEdge('c', 'e');
            }
            return graph;
        }
    }

    static class GetMergedGraph implements Function2<Graph, Graph, Graph>{
        public Graph call(Graph g1, Graph g2){
            Graph mergedGraph = new Graph();

            for (Map.Entry<Graph.Node, LinkedList<Graph.Node>> entry: g1.adjListMap.entrySet()){
                // add the vertex in the new graph
                mergedGraph.addVertex(entry.getKey().getData());

                // add all the adjacent nodes for this node from graph1
                for(Graph.Node node: entry.getValue()){
                    mergedGraph.addVertex(node.getData());
                    mergedGraph.addEdge(entry.getKey().getData(), node.getData());
                }

                //check if this node exists in graph2
                for (Map.Entry<Graph.Node, LinkedList<Graph.Node>> setEntry: g2.adjListMap.entrySet()){
                    if(setEntry.getKey().getData().equals(entry.getKey().getData())){
                        List<Graph.Node> adj = g2.adjListMap.get(setEntry.getKey());
                        for(Graph.Node n : adj){
                            mergedGraph.addVertex(setEntry.getKey().getData());
                            mergedGraph.addEdge(entry.getKey().getData(), setEntry.getKey().getData());
                        }
                    }
                }

                /*if (g2.adjListMap.containsKey(entry.getKey())){
                    //add all the adjacent nodes for this node from graph2
                    List<Graph.Node> adj = g2.adjListMap.get(entry.getKey());
                    for(Graph.Node n : adj){
                        mergedGraph.addEdge(entry.getKey().getData(), n.getData());
                    }
                }*/
            }
            return mergedGraph;
        }

    }


}
