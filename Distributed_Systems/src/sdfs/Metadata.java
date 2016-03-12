package sdfs;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import system.Catalog;

/**
 * Metadata is a thread-safe class that stores meta data information about SDFS.
 */
public class Metadata {
    private Map<String, Set<InetAddress>> fileLocations = new HashMap<>();
    private Map<InetAddress, Set<String>> filesOnNode = new HashMap<>();
    private Map<InetAddress, Datanode> IP2Datanode = new HashMap<>();
    /**
     * record the time a file is first added into the metadata, so that
     * replication check will not check recent files. So that when the file is
     * still being transmitted, it will not be mistakenly replicated.
     */
    private Map<String, Long> fileAddTime = new HashMap<>();

    private synchronized void cleanUp() {
        for (Iterator<Map.Entry<String, Set<InetAddress>>> itr = fileLocations.entrySet()
                .iterator(); itr.hasNext();) {
            Map.Entry<String, Set<InetAddress>> entry = itr.next();
            if (entry.getValue().isEmpty()) {
                itr.remove();
                fileAddTime.remove(entry.getKey());
            }
        }
    }

    private synchronized List<InetAddress> getKRandomNodesExcept(int k, InetAddress... addresses) {
        List<InetAddress> nodes = new ArrayList<>(filesOnNode.keySet());
        for (InetAddress address : addresses) {
            nodes.remove(address);
        }
        Collections.shuffle(nodes);
        return nodes.subList(0, Math.min(k, nodes.size()));
    }

    public synchronized List<Datanode> getKidlestNodes(int k) {
        List<InetAddress> nodes = new ArrayList<>(filesOnNode.keySet());
        nodes.sort(new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                return filesOnNode.get(o1).size() - filesOnNode.get(o2).size();
            }
        });

        List<Datanode> datanodes = new ArrayList<>();
        for (InetAddress IP : nodes.subList(0, Math.min(k, nodes.size()))) {
            datanodes.add(IP2Datanode.get(IP));
        }
        return datanodes;
    }

    public synchronized List<Datanode> getFileLocations(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations == null || locations.isEmpty()) {
            return new ArrayList<Datanode>();
        }

        List<Datanode> datanodes = new ArrayList<>();
        for (InetAddress IP : locations) {
            datanodes.add(IP2Datanode.get(IP));
        }
        return datanodes;
    }

    public synchronized List<InetAddress> getFileLocationIPs(String fileName) {
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations == null || locations.isEmpty()) {
            return new ArrayList<InetAddress>();
        }
        return new ArrayList<InetAddress>(locations);
    }

    public synchronized void deleteNode(InetAddress IP) {
        IP2Datanode.remove(IP);
        filesOnNode.remove(IP);
        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            entry.getValue().remove(IP);
        }
        cleanUp();
    }

    public synchronized void deleteNodes(List<InetAddress> addresses) {
        for (InetAddress address : addresses) {
            deleteNode(address);
        }
    }

    public synchronized void mergeBlockReport(BlockReport blockreport) {
        IP2Datanode.put(blockreport.getIPAddress(), blockreport.getDatanode());
        filesOnNode.put(blockreport.getIPAddress(), new HashSet<String>(blockreport.getFiles()));

        long curTime = System.currentTimeMillis();
        for (String file : blockreport.getFiles()) {
            fileAddTime.putIfAbsent(file, curTime);
            fileLocations.putIfAbsent(file, new HashSet<InetAddress>());
            fileLocations.get(file).add(blockreport.getIPAddress());
        }

        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            if (!blockreport.getFiles().contains(entry.getKey())) {
                entry.getValue().remove(blockreport.getIPAddress());
            }
        }
        cleanUp();
    }

    public synchronized Datanode getDatanode(InetAddress IP) {
        return IP2Datanode.get(IP);
    }

    public synchronized void deleteFile(String fileName) {
        fileAddTime.remove(fileName);
        Set<InetAddress> locations = fileLocations.get(fileName);
        if (locations != null) {
            for (InetAddress IP : locations) {
                filesOnNode.get(IP).remove(fileName);
            }
            fileLocations.remove(fileName);
        }
    }

    public synchronized void addFile(String file, InetAddress IP) {
        // if IP is not in IP2Datanode, which means this comes from last leader,
        // so just ignore it, because the information will be contained in
        // blockreport later.
        if (!IP2Datanode.containsKey(IP)) {
            return;
        }
        fileAddTime.putIfAbsent(file, System.currentTimeMillis());
        fileLocations.putIfAbsent(file, new HashSet<InetAddress>());
        fileLocations.get(file).add(IP);
        filesOnNode.get(IP).add(file);
    }

    /**
     * Check whether there is any file to be replicated.
     * 
     * @return all files that needs to be replicated. return format: String
     *         fileName, Datanode from, Datanode to...
     */
    public synchronized List<List<Object>> getReplicationRequest() {
        long curTime = System.currentTimeMillis();
        List<List<Object>> rep = new ArrayList<>();

        for (Map.Entry<String, Set<InetAddress>> entry : fileLocations.entrySet()) {
            if (curTime - fileAddTime.get(entry.getKey()) < Catalog.REPLICATION_SILENCE_PERIOD) {
                continue;
            }
            int numToReplicate = Catalog.REPLICATION_FACTOR - entry.getValue().size();
            if (numToReplicate > 0) {
                List<InetAddress> tos = getKRandomNodesExcept(numToReplicate,
                        entry.getValue().toArray(new InetAddress[entry.getValue().size()]));
                if (tos.isEmpty()) {
                    continue;
                } else {
                    List<Object> ret = new ArrayList<>();
                    ret.add(entry.getKey());
                    Datanode from = getDatanode(entry.getValue().iterator().next());
                    ret.add(from);
                    for (InetAddress IP : tos) {
                        Datanode to = getDatanode(IP);
                        ret.add(to);
                    }
                    rep.add(ret);
                }
            }
        }
        return rep;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s%n%s", filesOnNode, fileLocations.toString());
    }
}
