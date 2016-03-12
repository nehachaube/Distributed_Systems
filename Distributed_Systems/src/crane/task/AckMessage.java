package crane.task;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class AckMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * tupleID represents which tuple the ack message belongs to.
     */
    public final int tupleID;

    public final long checksum;

    public AckMessage(int tupleID, long checksum) {
        this.tupleID = tupleID;
        this.checksum = checksum;
    }

    /**
     * Java's serialization mechanism seems like to be bandwidth inefficient. So
     * I wrote a simple one for high performance.
     * 
     * @param bytes
     */
    public AckMessage(byte[] bytes) {
        int tid = ByteBuffer.wrap(bytes, 0, 4).getInt();
        long cs = ByteBuffer.wrap(bytes, 4, 8).getLong();

        this.tupleID = tid;
        this.checksum = cs;
    }

    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.putInt(tupleID);
        bb.putLong(checksum);
        return bb.array();
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", tupleID, checksum);
    }
}
