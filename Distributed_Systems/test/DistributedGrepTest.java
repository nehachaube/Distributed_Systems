import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DistributedGrepTest {

    /**
     * Assume grepResult and DistributedGrepResult already exist. grepResult is
     * the result of running grep locally on *one* file. DistributedGrepReulst
     * is the result of running grep distributedly on the same file, but split
     * across multiple servers.
     * 
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        File dgr = new File("distributedGrepResult");
        List<String> orderedLines = TestUtil.sortDistributedGrepResults(dgr);

        StringWriter sw1 = new StringWriter();
        PrintWriter pw = new PrintWriter(sw1, true);
        for (String line : orderedLines) {
            pw.println(line);
        }
        StringReader sr1 = new StringReader(sw1.toString());

        /*
         * Scanner sc = new Scanner(new InputStreamReader(new
         * FileInputStream("grepResult"), Catalog.encoding));
         * sc.useDelimiter("\\n|\\r\\n"); StringWriter sw2 = new StringWriter();
         * pw = new PrintWriter(sw2, true); while (sc.hasNext()) { String line =
         * sc.next(); pw.println(line.substring(line.indexOf(":") + 1)); }
         * sc.close();
         * 
         * StringReader sr2 = new StringReader(sw2.toString());
         * Assert.assertTrue(TestUtil.compareTwoFiles(sr1, sr2));
         */
        
        Reader r = new InputStreamReader(new FileInputStream("grepResult"));
        Assert.assertTrue(TestUtil.compareTwoFiles(sr1, r));
        r.close();
    }

}
