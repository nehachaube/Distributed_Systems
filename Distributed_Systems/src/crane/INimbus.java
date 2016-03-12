package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

import crane.topology.Topology;

/**
 * Nimbus is the master node for Crane. Nimbus is responsible for receiving job
 * from clients, assigning tasks to supervisors, and monitoring for failures.
 * Our implementation of Nimbus tracks at most one job at the same time, the
 * other jobs need to wait until the current job finishes, and we assume Nimbus
 * never dies. (This is a reasonable assumption.)
 */
public interface INimbus extends Remote {
    /**
     * Submit the job to Nimbus.
     * 
     * @param topology
     *            the topology for the job
     * @throws RemoteException
     * @throws IOException
     * @throws InterruptedException
     */
    void submitTopology(Topology topology)
            throws RemoteException, IOException, InterruptedException;

    /**
     * Supervisor join the system by calling the method.
     * 
     * @param selfIP
     *            the IP address of the caller supervisor
     * @param supervisor
     *            the Remote stub for the caller supervisor
     * @throws RemoteException
     */
    void registerSupervisor(InetAddress selfIP, ISupervisor supervisor) throws RemoteException;

    /**
     * Notify Nimbus that the current job has finished. The method is called by
     * spout task.
     * 
     * @throws RemoteException
     */
    void finishJob() throws RemoteException;
}
