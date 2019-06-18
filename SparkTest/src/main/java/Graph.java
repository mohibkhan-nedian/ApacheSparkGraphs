import java.io.Serializable;
import java.util.*;

/**
 * Created by Mohib on 4/9/2019.
 */
public class Graph implements Serializable {

    public static class Node implements Serializable{

        private Character data;

        private Node(Character c){
            this.data = c;
        }

        public Character getData() {
            return data;
        }

        public void setData(Character data) {
            this.data = data;
        }

        // equals and hashCode
        @Override
        public boolean equals(Object obj){
            if (this == obj)
                return true;
            if (!(obj instanceof Node))
                return false;
            return ((Node) obj).getData().equals(this.getData());
        }

        public int hashcode(){
            return this.getData().hashCode();
        }
    }

    HashMap<Node, LinkedList<Node>> adjListMap;

    Graph(){
        this.adjListMap = new HashMap<>();
    }

    public void addVertex(Character character){
        adjListMap.putIfAbsent(new Node(character), new LinkedList<Node>());
    }

    public void addEdge(Character a, Character b){
        int i =0;
        ArrayList<Node> arrayList = new ArrayList<>();
        for(Map.Entry<Node, LinkedList<Node>> entry: this.adjListMap.entrySet()){
            if(a.equals(entry.getKey().getData())){
                arrayList.add(i++, entry.getKey());
            }
            else if (b.equals(entry.getKey().getData())){
                arrayList.add(i++, entry.getKey());
            }
        }
        if(arrayList.get(0) != null && arrayList.get(1) != null){
            adjListMap.get(arrayList.get(0)).add(arrayList.get(1));
            adjListMap.get(arrayList.get(1)).add(arrayList.get(0));
        }
        //i think equals and hashcode will have to be implemented because n1 node is different from the node with same data in the map
        /*adjListMap.get(n1).add(n2);
        adjListMap.get(n2).add(n1);*/
    }

    public boolean containEdge(Character a, Character b){
        return this.adjListMap.get(new Node(a)).contains(new Node(b));
    }
    /*
    @Override
    public String toString(){
        String output = "";
        for (Map.Entry<Graph.Node, List<Graph.Node>> entry: this.adjListMap.entrySet()) {

        }
    }*/
}
