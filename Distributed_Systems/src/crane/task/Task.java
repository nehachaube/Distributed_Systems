package crane.task;

import java.io.Serializable;

import crane.topology.Address;
import crane.topology.IComponent;

/**
 * Task has a component field, and a number field, which represents the i-th
 * parallelism of the component.
 */
public class Task implements Serializable {
    private static final long serialVersionUID = 1L;

    public final IComponent comp;
    public final int no;

    public Task(IComponent comp, int no) {
        this.comp = comp;
        this.no = no;
    }

    public Address getTaskAddress() {
        return this.comp.getTaskAddress(no);
    }

    public String getTaskId() {
        return String.format("%s-%s", comp.getComponentID(), no);
    }
}
