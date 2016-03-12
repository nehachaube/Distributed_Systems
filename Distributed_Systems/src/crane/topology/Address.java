package crane.topology;

import java.io.Serializable;
import java.net.InetAddress;

public class Address implements Serializable {

    private static final long serialVersionUID = 1L;
    public final InetAddress IP;
    public final int port;

    public Address(InetAddress IP, int port) {
        this.IP = IP;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Address)) {
            return false;
        } else {
            Address that = (Address) o;
            return this.IP.equals(that.IP) && this.port == that.port;
        }
    }

    @Override
    public String toString() {
        return String.format("%s:%s", IP, port);
    }
}
