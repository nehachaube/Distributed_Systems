package system;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class CustomizedFormatter extends Formatter {
    @Override
    public String format(LogRecord record) {
        return String.format("[%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS:%1$tL] %2$s%3$s%n",
                record.getMillis(), formatMessage(record),
                record.getThrown() == null ? "" : record.getThrown());
    }
}
