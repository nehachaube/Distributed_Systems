package crane.partition;

import java.io.Serializable;

import crane.tuple.ITuple;

public interface IPartitionStrategy extends Serializable {
    int partition(ITuple tuple, int numTasks);
}
