package system;
import java.io.IOException;

public interface DaemonService {
    /**
     * Start the service as a daemon service, i.e. in other threads, and the
     * method will return immediately. If start an already started service, then
     * the result is unspecified.
     * 
     * @throws IOException
     *             if IO error occurs.
     */
    public void startServe() throws IOException;

    /**
     * Stop the service. If stop an already stopped service, then the result is
     * unspecified.
     */
    public void stopServe() throws Exception;
}
