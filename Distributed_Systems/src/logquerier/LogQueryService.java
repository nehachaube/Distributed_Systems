package logquerier;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.cli.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import system.Catalog;
import system.DaemonService;

public class LogQueryService implements DaemonService {

    private class GrepWorker implements Runnable {
        private final Socket socket;

        GrepWorker(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), Catalog.ENCODING));
                    OutputStream os = socket.getOutputStream()) {
                String line;
                while ((line = br.readLine()) != null) {
                    // We can always assume received message is valid,
                    // since we are only allowed to be contacted by
                    // client programs, and any mistakes should be detected
                    // there.
                    JSONArray cmd = (JSONArray) JSONValue.parse(line);
                    switch ((String) cmd.get(0)) {
                    case "grep":
                        @SuppressWarnings("unchecked")
                        String[] args = (String[]) cmd.subList(1, cmd.size())
                                .toArray(new String[0]);
                        ;
                        new Grep(args, os).execute();
                        return;
                    default:
                        // Should never reach here.
                        System.err.println(String.format("Unsupported Operation: %s.", cmd.get(0)));
                        return;
                    }
                }
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private ServerSocket serverSocket;

    @Override
    public void startServe() throws IOException {
        serverSocket = new ServerSocket(Catalog.LOG_QUERY_SERVICE_PORT);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        new Thread(new GrepWorker(serverSocket.accept())).start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void stopServe() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws IOException {
        LogQueryService lqs = new LogQueryService();
        lqs.startServe();
    }
}
