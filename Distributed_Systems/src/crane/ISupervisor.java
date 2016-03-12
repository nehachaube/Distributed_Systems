package crane;

import java.net.SocketException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import crane.task.Task;

/**
 * Supervisor listens for work assigned to its machine, and starts worker
 * threads based on what Nimbus has assigned to it.
 */
public interface ISupervisor extends Remote {
    /**
     * assign the specified task to the machine.
     * 
     * @param task
     * @throws RemoteException
     * @throws SocketException
     */
    void assignTask(Task task) throws RemoteException, SocketException;

    /**
     * notify the supervisor that information about the given task needs to be
     * updated.
     * 
     * @param task
     * @throws RemoteException
     */
    void updateTask(Task task) throws RemoteException;

    /**
     * Terminate all worker threads.
     */
    void terminateTasks() throws RemoteException;
}
