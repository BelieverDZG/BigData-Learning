package dzg.java.prost.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author BelieverDzg
 * @date 2020/12/9 14:32
 */
public class SGIndex {

    //(E,V)
    List<int[]> outGoingEdges;

    //(V,E)
    List<int[]> incomingEdges;

    Integer vertex;

    public SGIndex(List<int[]> outGoingEdges, List<int[]> incomingEdges, Integer vertex) {
        this.outGoingEdges = outGoingEdges;
        this.incomingEdges = incomingEdges;
        this.vertex = vertex;
    }

    @Override
    public String toString() {
        return "SGIndex{" +
                "outGoingEdges = " + outGoingEdges +
                ", incomingEdges = " + incomingEdges +
                ", vertex = " + vertex +
                '}';
    }

    public List<int[]> getOutGoingEdges() {
        return outGoingEdges;
    }

    public void setOutGoingEdges(List<int[]> outGoingEdges) {
        this.outGoingEdges = outGoingEdges;
    }

    public List<int[]> getIncomingEdges() {
        return incomingEdges;
    }

    public void setIncomingEdges(List<int[]> incomingEdges) {
        this.incomingEdges = incomingEdges;
    }

    public Integer getVertex() {
        return vertex;
    }

    public void setVertex(Integer vertex) {
        this.vertex = vertex;
    }
}
