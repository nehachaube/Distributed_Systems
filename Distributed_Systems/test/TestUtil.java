import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import system.Catalog;

public class TestUtil {

    /**
     * Sort the results of distributed grep according to machine number where
     * they come from, and strip prefix of lines. Notice: Only apply to results
     * of one file. This method is only used for testing.
     * 
     * @param file
     *            the file which stores the results of distributed grep.
     * @return sorted results of grep results.
     * @throws IOException
     *             If any error occurs when reading
     */
    public static List<String> sortDistributedGrepResults(File file) throws IOException {
        List<String> lines = new ArrayList<>();

        // Use Scanner rather than BufferedReader
        try (Scanner sc = new Scanner(new InputStreamReader(new FileInputStream(file), Catalog.ENCODING))) {
            sc.useDelimiter("\\n|\\r\\n");
            Map<Integer, List<String>> machineLines = new HashMap<>();

            while (sc.hasNext()) {
                String line = sc.next();
                int t = line.indexOf(":");
                int machine = Integer.parseInt(line.substring(0, t).replaceAll("[^0-9]", ""));
                String content = line.substring(line.indexOf(":", t + 1) + 1);

                if (!machineLines.containsKey(machine)) {
                    machineLines.put(machine, new ArrayList<String>());
                }
                machineLines.get(machine).add(content);
            }

            Integer[] machineNos = machineLines.keySet().toArray(new Integer[0]);
            Arrays.sort(machineNos);
            for (int i : machineNos) {
                lines.addAll(lines.size(), machineLines.get(i));
                // To avoid memory leak
                machineLines.remove(i);
            }
        }

        return lines;
    }

    /**
     * @param source1
     * @param source2
     * @return <code>true</code> if the contents of two files are exactly the
     *         same, else <code>false</code>
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static boolean compareTwoFiles(Readable source1, Readable source2) throws FileNotFoundException {
        try (Scanner sc1 = new Scanner(source1); Scanner sc2 = new Scanner(source2)) {

            sc1.useDelimiter("\\n|\\r\\n");
            sc2.useDelimiter("\\n|\\r\\n");
            while (sc1.hasNext() && sc2.hasNext()) {
                String line1 = sc1.next();
                String line2 = sc2.next();

                if (!line1.equals(line2)) {
                    return false;
                }
            }

            if (sc1.hasNext() || sc2.hasNext()) {
                return false;
            }

            return true;
        }
    }

    // Basic unit test
    public static void main(String[] args) throws IOException {
        List<String> lines = TestUtil.sortDistributedGrepResults(new File("123"));
        for (String line : lines) {
            System.out.println(line);
        }
    }
}
