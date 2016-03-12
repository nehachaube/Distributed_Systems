package crane.bolt;

import java.io.IOException;
import java.util.List;

import crane.topology.IComponent;
import crane.tuple.ITuple;

public interface IBolt extends IComponent {
    List<ITuple> map(ITuple tuple) throws IOException;
}
