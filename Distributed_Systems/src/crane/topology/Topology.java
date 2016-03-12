package crane.topology;

import java.io.Serializable;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import crane.spout.ISpout;

public class Topology implements Iterable<IComponent>, Serializable {

    private static final long serialVersionUID = 1L;
    public final String topologyID;
    private final ISpout spout;

    public int size() {
        int size = 0;
        for (IComponent comp : this) {
            size += comp.getParallelism();
        }
        return size;
    }

    public Topology(String topologyID, ISpout spout) {
        this.topologyID = topologyID;
        this.spout = spout;
    }

    public ISpout getSpout() {
        return this.spout;
    }

    private class TopologyIterator implements Iterator<IComponent> {
        private final Deque<IComponent> queue;

        TopologyIterator() {
            queue = new LinkedList<>();
            queue.addLast(spout);
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public IComponent next() {
            IComponent comp = queue.removeFirst();
            for (IComponent child : comp.getChildren()) {
                queue.addLast(child);
            }
            return comp;
        }
    }

    @Override
    public Iterator<IComponent> iterator() {
        return new TopologyIterator();
    }

}
