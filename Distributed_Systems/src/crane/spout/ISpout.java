package crane.spout;

import java.io.IOException;

import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;

public interface ISpout extends IComponent {
    void open() throws IOException;

    void close() throws IOException;

    ITuple nextTuple() throws IOException;

    default Address getAddress() {
        return getTaskAddress(0);
    }
}
