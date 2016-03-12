package crane;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.task.Task;
import crane.topology.Address;
import crane.topology.IComponent;
import crane.topology.Topology;
import membershipservice.GossipGroupMembershipService;
import system.Catalog;
import system.CommonUtils;
import system.Identity;

public class Nimbus implements INimbus, Observer {

    private Registry registry;
    private Topology topology;
    private final GossipGroupMembershipService ggms;
    private Map<InetAddress, List<Task>> taskTracker;
    private Map<InetAddress, ISupervisor> supervisors;
    private Map<InetAddress, Integer> availablePorts;
    private Acker acker;

    /**
     * the mutex is used to ensure only one job is executed at one time
     */
    private Semaphore mutex;

    private final Logger nimbusLogger = CommonUtils.initializeLogger(Nimbus.class.getName(),
            Catalog.LOG_DIR + Catalog.NIMBUS_LOG, true);
    private final Logger ackerLogger = CommonUtils.initializeLogger(Acker.class.getName(),
            Catalog.LOG_DIR + Catalog.ACKER_LOG, false);

    public Nimbus() throws IOException {
        mutex = new Semaphore(1);

        ggms = new GossipGroupMembershipService(InetAddress.getLocalHost(),
                Catalog.CRANE_MEMBERSHIP_SERVICE_PORT, Catalog.CRANE_MEMBERSHIP_SERVICE_PORT);
        ggms.addObserver(this);
        taskTracker = new HashMap<>();
        supervisors = new HashMap<>();
        availablePorts = new HashMap<>();

        ggms.startServe();

        INimbus stub = (INimbus) UnicastRemoteObject.exportObject(this, 0);
        registry = LocateRegistry.createRegistry(Catalog.NIMBUS_PORT);
        registry.rebind("nimbus", stub);

        topology = null;
    }

    @Override
    public synchronized void submitTopology(Topology topology)
            throws RemoteException, InterruptedException, IOException {
        mutex.acquire();

        nimbusLogger.info(topology.topologyID + ": job received.");
        this.topology = topology;

        List<Task> tasks = new ArrayList<>();
        for (IComponent comp : topology) {
            for (int i = 0; i < comp.getParallelism(); i++) {
                tasks.add(new Task(comp, i));
            }
        }

        nimbusLogger.info("Assigning tasks.");
        assignTasks(tasks);

        nimbusLogger.info("Starting acker.");
        acker = new Acker(topology.getSpout().getAddress(), Catalog.ACKER_PORT, ackerLogger);
        new Thread(acker).start();

        // assign tasks in reverse order to ensure downstream components start
        // before upstream components, otherwise some initial tuples will be
        // wasted.
        for (int i = tasks.size() - 1; i >= 0; i--) {
            Task task = tasks.get(i);
            ISupervisor sv = supervisors.get(task.getTaskAddress().IP);
            try {
                sv.assignTask(task);
            } catch (RemoteException | SocketException e) {
                // failed assignment will be detected and reassigned later
                nimbusLogger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    // This method needs to be synchronized in order to avoid supervisors being
    // changed when assigning tasks
    private synchronized void assignTasks(List<Task> tasks) {
        int remainTasks = tasks.size();
        int k = 0;
        int i = 0;

        for (InetAddress ip : supervisors.keySet()) {
            int numTask = (int) Math.ceil(remainTasks / (double) (supervisors.size() - i++));
            int t = availablePorts.get(ip);
            availablePorts.put(ip, t + numTask);

            for (int j = 0; j < numTask; j++) {
                Task task = tasks.get(k++);

                InetAddress oldIP = task.getTaskAddress() != null ? task.getTaskAddress().IP : null;
                task.comp.assign(task.no, new Address(ip, t + j));
                taskTracker.get(ip).add(task);

                nimbusLogger.info(String.format("Assign %s: %s -> %s.", task.getTaskId(),
                        oldIP == null ? "" : oldIP.toString(), ip));
            }
            remainTasks -= numTask;
        }
    }

    @Override
    public synchronized void update(Observable o, Object arg) {
        List<Task> failedTasks = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<Identity> failedNodes = (ArrayList<Identity>) arg;
        for (Identity id : failedNodes) {
            nimbusLogger.info(id.IPAddress + " failed.");

            InetAddress ip = id.IPAddress;
            supervisors.remove(ip);
            for (Task task : taskTracker.get(ip)) {
                failedTasks.add(task);
            }
            taskTracker.remove(ip);
            availablePorts.remove(ip);
        }

        if (supervisors.isEmpty()) {
            if (topology != null) {
                nimbusLogger.info("All supervisors died. " + topology.topologyID + ": job failed.");
                cleanUp();
            }
            return;
        }

        assignTasks(failedTasks);

        for (Task task : failedTasks) {
            IComponent parent = task.comp.getParent();
            if (parent == null) {
                // it is spout. update Acker first!
                acker.setSpoutAddress(task.getTaskAddress());
            }

            ISupervisor sv = supervisors.get(task.getTaskAddress().IP);
            try {
                sv.assignTask(task);
            } catch (RemoteException | SocketException e) {
                nimbusLogger.log(Level.SEVERE, e.getMessage(), e);
            }

            if (parent != null) {
                for (int i = 0; i < parent.getParallelism(); i++) {
                    Task pt = new Task(parent, i);
                    ISupervisor psv = supervisors.get(pt.getTaskAddress().IP);
                    try {
                        psv.updateTask(pt);
                    } catch (RemoteException e) {
                        nimbusLogger.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            }
        }
    }

    @Override
    public synchronized void registerSupervisor(InetAddress ip, ISupervisor supervisor)
            throws RemoteException {
        nimbusLogger.info("Supervisor " + ip + " joined.");
        supervisors.put(ip, supervisor);
        taskTracker.put(ip, new ArrayList<>());
        availablePorts.put(ip, Catalog.WORKER_PORT_RANGE);
    }

    @Override
    public synchronized void finishJob() throws RemoteException {
        nimbusLogger.info(topology.topologyID + ": job finished.");
        cleanUp();
    }

    private synchronized void cleanUp() {
        for (InetAddress add : supervisors.keySet()) {
            ISupervisor sv = supervisors.get(add);
            try {
                sv.terminateTasks();
            } catch (RemoteException e) {
                nimbusLogger.log(Level.SEVERE, e.getMessage(), e);
            }
            taskTracker.get(add).clear();
        }
        acker.terminate();

        this.topology = null;
        for (InetAddress ip : availablePorts.keySet()) {
            availablePorts.put(ip, Catalog.WORKER_PORT_RANGE);
        }

        mutex.release();
    }

    public static void main(String[] args) throws IOException {
        new Nimbus();
    }
}
