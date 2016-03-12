package crane.topology;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.task.OutputCollector;
import crane.tuple.ITuple;

public interface IComponent extends Serializable {
    public String getComponentID();

    public void addChild(IComponent comp);

    public List<IComponent> getChildren();

    public IComponent getParent();

    public void setParent(IComponent comp);

    public int getParallelism();

    public void assign(int taskNo, Address address);

    public Address getTaskAddress(int taskNo);

    public void execute(ITuple tuple, OutputCollector output) throws IOException, InterruptedException;

    public IPartitionStrategy getPartitionStrategy();
}
