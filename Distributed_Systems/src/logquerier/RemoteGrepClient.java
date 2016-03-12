package logquerier;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.ParseException;
import org.json.simple.JSONValue;

import system.Catalog;

/**
 * RemoteGrepClient is a client program which sends `grep' command to servers,
 * and output matched lines received from servers to standard output. The usage
 * of RemoteGrepClient is exactly the same as Grep, except that at least one
 * file should be given (i.e. cannot read from standard input, which does not
 * make sense in distributed settings).
 */
public class RemoteGrepClient {

    private static class QueryThread implements Runnable {
        private final String host;
        private final String[] args;

        QueryThread(String host, String[] args) {
            this.host = host;
            this.args = args;
        }

        /**
         * Attempt to connect the specified host.
         * 
         * @return the connected socket if succeed, <code>null</code> otherwise.
         */
        private Socket connect() {
            try {
                Socket socket = new Socket(host, Catalog.LOG_QUERY_SERVICE_PORT);
                System.err.println(String.format("%s: Connection set up successfully.", host));
                return socket;
            } catch (IOException e) {
                System.err.println(String.format("%s: Failed to establish connection.", host));
            }
            return null;
        }

        /**
         * Send `grep' query over the specified socket, and output received
         * responses to standard output.
         * 
         * @param socket
         *            requires the socket not null and open.
         */
        private void executeQuery(Socket socket) {
            // Use scanner instead of BufferedReader to avoid insidious Carriage
            // Return problems
            try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Catalog.ENCODING),
                    true);
                    Scanner sc = new Scanner(new InputStreamReader(socket.getInputStream(), Catalog.ENCODING))
                            .useDelimiter("\\n|\\r\\n")) {
                // Notice: A tricky detail here.
                // send String array using JSON format
                // In case String has quote or space
                List<String> argList = new ArrayList<>();
                argList.add("grep");
                argList.addAll(Arrays.asList(args));
                String jsonText = JSONValue.toJSONString(argList);
                pw.println(jsonText);

                while (sc.hasNext()) {
                    String matchedLine = sc.next();
                    System.out.println(String.format("%s:%s", host, matchedLine));
                }
            } catch (IOException e) {
                System.err.println(String.format("%s: %s", host, e.getMessage()));
            }
        }

        private void close(Socket socket) {
            try {
                // flush System.out to ensure log lines appear after regular
                // lines.
                System.out.flush();
                socket.close();
            } catch (IOException e) {
            } finally {
                System.err.println(String.format("%s: Connection closed", host));
            }
        }

        @Override
        public void run() {
            Socket socket = connect();

            if (socket != null) {
                long startTime = System.nanoTime();

                executeQuery(socket);
                close(socket);

                long duration = System.nanoTime() - startTime;
                System.err.println(String.format("%s: Elapsed time: %ss", host, duration / 1000000000.));
            }
        }
    }
    
    public static void main(String[] args) throws IOException {
        // args errors are detected here,
        // so no invalid commands will be sent to other servers.
        try {
            Grep grep = new Grep(args, null);

            if (grep.getFileNamePatterns().isEmpty()) {
                System.err.println("RemoteGrepClient requires at least one file as arguments.");
                System.exit(-1);
            }

            for (String host : Catalog.HOST_LIST) {
                new Thread(new QueryThread(host, args)).start();
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            Grep.printHelp();
            System.exit(-1);
        }
    }

}
