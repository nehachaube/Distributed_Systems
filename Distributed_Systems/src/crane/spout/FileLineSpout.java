package crane.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;
import system.Catalog;

public class FileLineSpout extends BasicSpout {

    private static final long serialVersionUID = 1L;

    private final String fileName;
    private transient BufferedReader reader;
    
    public FileLineSpout(String componentID, String fileName, int sendGap) {
        super(componentID, sendGap);
        this.fileName = fileName;
    }

    @Override
    public void open() throws IOException {
        reader = new BufferedReader(new FileReader(Catalog.CRANE_DIR + fileName));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public ITuple nextTuple() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            OneStringTuple tuple = new OneStringTuple(0, line);
            return tuple;
        } else {
            return null;
        }
    }
}
