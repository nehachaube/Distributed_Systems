package crane.partition;

import crane.tuple.ITuple;

public class HashPartitionStrategy implements IPartitionStrategy {

    private static final long serialVersionUID = 1L;

    @Override
    public int partition(ITuple tuple, int numTasks) {
        return (tuple.hashCode() & 0x7FFFFFFF) % numTasks;
    }
}
