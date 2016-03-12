package logquerier;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import system.Catalog;

/**
 * Grep is a utility which is like UNIX command: egrep.
 */
public class Grep {
    private static final Options OPTIONS = Grep.buildGrepOptions();

    private final OutputStream os;
    private final Pattern pattern;
    private final List<String> fileNamePatterns;
    private final CommandLine cmd;

    private final boolean invertMatchToggle;
    private final boolean countToggle;
    private final boolean lineNumberToggle;
    private final int maxCount;
    private int currentCount;

    /**
     * Print help message to stderr.
     */
    public static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax = "java Grep [-options] [pattern] [file ...]";
        String header = String.format("%n%s", "The following options are available:");
        formatter.printHelp(new PrintWriter(System.err, true), 74, cmdLineSyntax, header,
                Grep.OPTIONS, 1, 3, null);
    }

    /**
     * Print a brief usage message to stderr.
     */
    public static void printUsage() {
        System.err.println("usage: java Grep [-cinvwx] [-e pattern] [-m num] [pattern] [file ...]");
    }

    private static Options buildGrepOptions() {
        Options options = new Options();
        Grep.addBooleanOptions(options);
        Grep.addArgumentOptions(options);
        return options;
    }

    private static void addBooleanOptions(Options options) {
        options.addOption("c", "count", false,
                "Only a count of selected lines is written to standard output.");
        options.addOption("i", "ignore-case", false,
                "Perform case insensitive matching.  By default, grep is case sensitive.");
        options.addOption("n", "line-number", false,
                "Each output line is preceded by its relative line number in the "
                        + "file, starting at line 1.  The line number counter is reset for each file processed.");
        options.addOption("v", "invert-match", false,
                "Selected lines are those not matching any of the specified patterns.");
        options.addOption("w", "word-regexp", false,
                "Select only those lines containing matches that form whole words.");
        options.addOption("x", "line-regexp", false,
                "Only input lines selected against an entire fixed string or"
                        + " regular expression are considered to be matching lines.");
    }

    private static void addArgumentOptions(Options options) {
        options.addOption(Option.builder("e").longOpt("regexp").hasArg().argName("pattern")
                .desc("Specify a pattern used during the search of the input: an "
                        + "input line is selected if it matches any of the specified "
                        + "patterns.  This option is most useful when multiple -e "
                        + "options are used to specify multiple patterns, or when a "
                        + "pattern begins with a dash (`-').")
                .build());
        options.addOption(Option.builder("m").longOpt("max-count").hasArg().argName("num")
                .desc("Stop reading the file after <num> matches.").build());
    }

    /**
     * Construct Grep from specified options and will output matched lines to
     * the specified output.
     * 
     * @param options
     *            options for Grep.
     * @param os
     *            the OutputStream to which matched lines should be written to.
     * @throws ParseException
     *             if any of the specified options is not valid.
     */
    public Grep(String[] options, OutputStream os) throws ParseException {
        try {
            this.os = os;
            CommandLineParser parser = new DefaultParser();
            this.cmd = parser.parse(Grep.OPTIONS, options);

            List<String> argList = cmd.getArgList();

            String[] regexps = cmd.getOptionValues("e");
            String regexp;
            if (regexps != null) {
                regexp = "(" + String.join("|", regexps) + ")";
                this.fileNamePatterns = argList;
            } else {
                regexp = "(" + argList.get(0) + ")";
                this.fileNamePatterns = argList.subList(1, argList.size());
            }

            int flags = 0;

            if (cmd.hasOption("word-regexp")) {
                regexp = "\\b" + regexp + "\\b";
            }
            if (cmd.hasOption("line-regexp")) {
                regexp = "^" + regexp + "$";
            } else {
                // Very very tricky here!
                // by default, . does not match newline including \r, \n, etc.
                regexp = ".*" + regexp + ".*";
                flags |= Pattern.UNIX_LINES;
            }
            if (cmd.hasOption("ignore-case")) {
                flags |= Pattern.CASE_INSENSITIVE;
            }

            this.pattern = Pattern.compile(regexp, flags);

            this.invertMatchToggle = cmd.hasOption("invert-match");
            this.countToggle = cmd.hasOption("count");
            this.lineNumberToggle = cmd.hasOption("line-number");
            this.currentCount = 0;
            if (cmd.hasOption("max-count")) {
                this.maxCount = Integer.parseInt(cmd.getOptionValue("max-count"));
            } else {
                this.maxCount = Integer.MAX_VALUE;
            }

        } catch (NumberFormatException e) {
            throw new ParseException("grep: Invalid arguement");
        } catch (IndexOutOfBoundsException e) {
            throw new ParseException("grep: No pattern specified");
        } catch (ParseException e) {
            throw new ParseException("grep: " + e.getMessage());
        }
    }

    /**
     * @return the concatenation of all patterns.
     */
    public String getPattern() {
        return this.pattern.toString();
    }

    /**
     * @return the specified file name patterns in the command
     */
    public List<String> getFileNamePatterns() {
        return this.fileNamePatterns;
    }

    /**
     * @return the parsed options in a CommandLine object
     */
    public CommandLine getOptions() {
        return this.cmd;
    }

    private void grep(Scanner sc, PrintWriter pw, String prefix) {
        int countMatches = 0;
        int lineNumber = 1;
        while (sc.hasNext()) {
            String line = sc.next();
            if (pattern.matcher(line).matches() ^ invertMatchToggle) {
                countMatches += 1;
                currentCount += 1;
                if (!this.countToggle) {
                    if (this.lineNumberToggle) {
                        pw.println(String.format("%s%s:%s", prefix, lineNumber, line));
                    } else {
                        pw.println(prefix + line);
                    }
                }

                if (currentCount >= this.maxCount) {
                    return;
                }
            }
            lineNumber++;
        }

        if (this.countToggle) {
            pw.println(prefix + countMatches);
        }
    }

    /**
     * @param fileNamePatterns
     * @return the file names of all files that match the specified file name
     *         patterns
     */
    private static List<String> getTargetFiles(List<String> fileNamePatterns) {
        List<String> targetFiles = new ArrayList<>();

        for (String pattern : fileNamePatterns) {
            File file = new File(pattern);
            String name = file.getName();
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + name);
            String dirPrefix = file.getParent() == null ? "" : file.getParent() + File.separator;
            File dir = file.getParentFile();

            if (dir == null) {
                dir = new File(System.getProperty("user.dir"));
            }

            boolean matched = false;
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    String fn = f.getName();
                    if (matcher.matches(Paths.get(fn))) {
                        matched = true;
                        if (f.isDirectory()) {
                            System.err.println(
                                    String.format("grep: %s: Is a directory", dirPrefix + fn));
                        } else {
                            targetFiles.add(dirPrefix + fn);
                        }
                    }
                }
            }

            if (!matched) {
                System.err.println(String.format("grep: %s: No such file or directory", file));
            }

        }

        return targetFiles;
    }

    public void execute() {
        PrintWriter pw;
        try {
            pw = new PrintWriter(new OutputStreamWriter(os, Catalog.ENCODING), true);
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
            return;
        }
        // Use Scanner instead of BufferedReader.
        // Because Scanner can specify delimiter which BufferedReader cannot.
        // BufferedReader will use \r as a delimiter, which is not what we want.
        Scanner sc;

        if (fileNamePatterns.isEmpty()) {
            try {
                sc = new Scanner(new InputStreamReader(System.in, Catalog.ENCODING));
                sc.useDelimiter("\\n|\\r\\n");
                grep(sc, pw, "");
            } catch (UnsupportedEncodingException e) {
                // Should never reach here
                e.printStackTrace();
            }
        } else {
            List<String> targetFiles = Grep.getTargetFiles(fileNamePatterns);

            for (String fileName : targetFiles) {
                String prefix = fileName + ":";
                try {
                    sc = new Scanner(
                            new InputStreamReader(new FileInputStream(fileName), Catalog.ENCODING));
                    sc.useDelimiter("\\n|\\r\\n");
                    grep(sc, pw, prefix);
                    sc.close();

                    if (this.currentCount >= this.maxCount) {
                        return;
                    }
                } catch (FileNotFoundException e) {
                    System.err.println("grep: +" + prefix + " No such file or directory");
                } catch (UnsupportedEncodingException e) {
                    // Should never reach here
                    e.printStackTrace();
                }
            }
        }
    }

    // for command line use
    public static void main(String args[]) {
        try {
            Grep grep = new Grep(args, System.out);
            grep.execute();
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            Grep.printHelp();
            System.exit(-1);
        }
    }
}
