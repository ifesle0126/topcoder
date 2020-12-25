import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 框架主函数
 * 
 * @author gaosiyu
 * @date 2020年11月13日 上午9:35:32
 */
public class MainFrame {

    private static BufferedWriter bw = null;
    private static final char NEWLINE = '\n';

    public static void main(String[] args) throws Exception {
        if (!argsCheck(args)) {
            System.err.println("params not match: " + Arrays.toString(args)
                    + ", need inputfileDir outputfile useTimeFile tempdir!");
            System.exit(0);
        }

        // 输入文件目录，包含3个文件，seed.txt, all.txt, count.txt
        final String inputFileDir = args[0];
        // 输出结果集文件
        final String outputFile = args[1];
        // 执行用时文件，计时单位ms
        final String useTimeFile = args[2];
        // 计算用临时目录
        final String tempDir = args[3];

        long totalTime = 0;

        try {

            bw = Files.newBufferedWriter(Paths.get(outputFile), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

            final String seedFile = inputFileDir + "/seed.txt";
            final String allFile = inputFileDir + "/all.txt";
            final String countFile = inputFileDir + "/count.txt";

            int outputCount = getOutputCount(countFile);

            Solution solution = new Solution();

            // 执行主体，计时单位ms
            long startTime = System.currentTimeMillis();
            solution.process(seedFile, allFile, outputCount, tempDir);
            long endTime = System.currentTimeMillis();

            // 计时
            totalTime = endTime - startTime;
            Files.write(Paths.get(useTimeFile), String.valueOf(totalTime).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        } finally {
            close(bw);
        }
        System.exit(0);
    }

    public static void addSet(final String line) throws Exception {
        bw.write(line);
        bw.write(NEWLINE);
    }

    public static void addSet(final String[] result) throws Exception {
        for (String line : result) {
            bw.write(line);
            bw.write(NEWLINE);
        }
    }

    public static void addSet(final List<String> result) throws Exception {
        for (String line : result) {
            bw.write(line);
            bw.write(NEWLINE);
        }
    }

    public static void addSet(final Set<String> result) throws Exception {
        for (String line : result) {
            bw.write(line);
            bw.write(NEWLINE);
        }
    }

    public static void addSet(final Iterator<String> it) throws Exception {
        while (it.hasNext()) {
            bw.write(it.next());
            bw.write(NEWLINE);
        }
    }

    private static boolean argsCheck(String[] str) {
        return str == null || str.length == 4;
    }

    private static int getOutputCount(String countFile) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get(countFile));
        return Integer.parseInt(lines.get(0));
    }

    private static void close(Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }
}