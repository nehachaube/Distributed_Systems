package crane.tuple;

import java.util.concurrent.ThreadLocalRandom;

public class BasicTuple implements ITuple {

    private static final long serialVersionUID = 1L;

    private int tupleID;
    protected Object[] content;
    private long salt = 0;

    public BasicTuple(int tupleID) {
        this.tupleID = tupleID;
    }

    @Override
    public int getID() {
        return tupleID;
    }

    @Override
    public long getSalt() {
        return salt;
    }

    @Override
    public Object[] getContent() {
        return this.content;
    }

    @Override
    public void setSalt() {
        salt = ThreadLocalRandom.current().nextLong();
    }

    @Override
    public void setID(int id) {
        tupleID = id;
    }
}
