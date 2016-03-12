package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import crane.bolt.IBolt;
import crane.task.BoltWorker;
import crane.task.CraneWorker;
import crane.task.SpoutWorker;
import crane.task.Task;
import crane.topology.Address;
import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CommonUtils;

public class Supervisor implements ISupervisor {

    private final Address ackerAddress;
    private final GossipGroupMembershipService ggms;
    private final Map<String, CraneWorker> taskTracker;
    private final INimbus nimbus;
    private final Logger logger = CommonUtils.initializeLogger(Supervisor.class.getName(),
            Catalog.LOG_DIR + Catalog.SUPERVISOR_LOG, true);

    public Supervisor() throws NotBoundException, IOException {
        // For simplicity, we assume acker is running on the same machine as Nimbus
        ackerAddress = new Address(InetAddress.getByName(Catalog.NIMBUS_ADDRESS),
                Catalog.ACKER_PORT);

        ggms = new GossipGroupMembershipService(InetAddress.getByName(Catalog.NIMBUS_ADDRESS),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
        taskTracker = new HashMap<>();
        
        ggms.startServe();
        Registry registry = LocateRegistry.getRegistry(Catalog.NIMBUS_ADDRESS, Catalog.NIMBUS_PORT);
        nimbus = (INimbus) registry.lookup("nimbus");

        ISupervisor stub = (ISupervisor) UnicastRemoteObject.exportObject(this,
                Catalog.SUPERVISOR_PORT);
        nimbus.registerSupervisor(InetAddress.getLocalHost(), stub);
    }

    @Override
    public void assignTask(Task task) throws RemoteException, SocketException {
        CraneWorker worker;
        if (task.comp instanceof IBolt) {
            worker = new BoltWorker(task, ackerAddress, logger);
        } else {
            worker = new SpoutWorker(task, ackerAddress, nimbus, logger);
        }

        logger.info(String.format("%s assigned.", task.getTaskId()));
        taskTracker.put(task.getTaskId(), worker);
        new Thread(worker).start();
    }

    @Override
    public void updateTask(Task task) throws RemoteException {
        CraneWorker worker = taskTracker.get(task.getTaskId());
        if (worker != null) {
            taskTracker.get(task.getTaskId()).setTask(task);
        }
    }

    @Override
    public void terminateTasks() {
        for (CraneWorker worker : taskTracker.values()) {
            worker.terminate();
        }
        taskTracker.clear();
    }

    public static void main(String[] args) throws IOException, NotBoundException {
        new Supervisor();
    }
}
