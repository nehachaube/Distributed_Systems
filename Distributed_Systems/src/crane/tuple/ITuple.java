package crane.tuple;

import java.io.Serializable;

public interface ITuple extends Serializable {
    int getID();
    
    void setID(int id);

    long getSalt();

    Object[] getContent();
    
    void setSalt();
}
