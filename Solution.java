import javafx.util.Pair;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * 请在此类中完成解决方案，实现process完成数据的处理逻辑。
 *
 * @author gaosiyu
 * @date 2020年11月13日 上午9:35:32
 */
public class Solution {

//    private static final ExecutorService executor = new ThreadPoolExecutor(32, 32,
//            0, TimeUnit.MILLISECONDS,
//            new LinkedBlockingQueue<Runnable>());

    private static final ExecutorService executor = Executors.newFixedThreadPool(4);

    /**
     * 主体逻辑实现demo，实现代码时请注意逻辑严谨性，涉及到操作文件时，保证文件有开有闭等。
     *
     * @param seedFile    种子集文件
     * @param allFile     候选集文件
     * @param outputCount 需要输出的结果数量
     * @param tempDir     临时文件存放目录
     */
    public void process(String seedFile, String allFile, int outputCount, String tempDir) throws Exception {
        List<Pair<String, float[]>> seedLines = new ArrayList<>();
        Scanner seedScanner = new Scanner(new File(seedFile));
        while (seedScanner.hasNext()) {
            String line = seedScanner.nextLine();
            String[] cells = line.split(",");
            String key = cells[0];
            float[] vector = new float[cells.length - 1];
            for (int i = 1; i < cells.length; i++) {
                vector[i - 1] = Float.parseFloat(cells[i]);
            }
            seedLines.add(new Pair<>(key, vector));
        }

        Scanner allScanner = new Scanner(new File(allFile));
        LinkedList<String> link = new LinkedList<>();
        ArrayList<Float> array = new ArrayList<>();
        while (allScanner.hasNext()) {
            String line = allScanner.nextLine();
            String[] allArr = line.split(",");
            String key = allArr[0];
            float[] vector = new float[allArr.length - 1];
            for (int i = 1; i < allArr.length; i++) {
                vector[i - 1] = Float.parseFloat(allArr[i]);
            }
            float maxScore = 0;
            String maxId = null;
            for (int i = 0; i < seedLines.size(); i++) {
//                float d = similarParallel(seedLines.get(i).getValue(), vector);
                float d = similar(seedLines.get(i).getValue(), vector);
//                System.out.println(d);
                if (d > maxScore) {
                    maxScore = d;
                    maxId = key;
                }
            }
            if (maxId != null && !maxId.isEmpty()) {
                addQueue(link, array, maxId, maxScore, outputCount);
            }
        }
        MainFrame.addSet(link);

/*        String[] result = new String[]{"0X123", "0X124", "0X253", "0X16C"};

        //请通过此方法输出答案,多次调用会追加记录
        MainFrame.addSet(result);*/
    }

    private void addQueue(LinkedList<String> queue, List<Float> sorted, String id, float score, int count) {
        if (queue.size() == 0) {
            queue.add(id);
            sorted.add(score);
            return;
        }
        int beg = 0;
        int end = queue.size() - 1;
        while (beg <= end) {
            int mid = (end + beg) / 2;
            if (sorted.get(mid) < score) {
                end = mid - 1;
            } else if (sorted.get(mid) > score) {
                beg = mid + 1;
            } else {
                beg++;
            }
        }
        queue.add(beg, id);
        if (queue.size() > count) {
            queue.removeLast();
        }
        float f = score;
        for (int i = beg; i < count; i++) {
            if (i == sorted.size()) {
                sorted.add(f);
                break;
            } else {
                float tmp = sorted.get(i);
                sorted.set(i, f);
                f = tmp;
            }
        }
    }

    private float similarParallel(float[] seedArr, float[] inArr) {
        int step = 256;
        List<Future<Float[]>> l = new ArrayList<>();
        for (int i = 0; i + step - 1 < seedArr.length; i += step) {
            Future<Float[]> d = executor.submit(new VectorMultiThread(seedArr, inArr, i, i + step - 1));
            l.add(d);
        }
        float sum = 0;
        float aPow = 0;
        float bPow = 0;
        try {
            for (Future<Float[]> df : l) {
                Float[] ds = df.get();
                sum += ds[0];
                aPow += ds[1];
                bPow += ds[2];
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return sum / (float)(Math.sqrt(aPow) * Math.sqrt(bPow));
    }

    private class VectorMultiThread implements Callable<Float[]> {

        private float[] as;
        private float[] bs;
        private int beg;
        private int end;

        public VectorMultiThread(float[] as, float[] bs, int beg, int end) {
            this.as = as;
            this.bs = bs;
            this.beg = beg;
            this.end = end;
        }

/*        @Override
        public Double[] call() throws Exception {
            double sum = 0;
            double aPow = 0;
            double bPow = 0;
            for (int i = beg; i <= end && i < as.length && i < bs.length; i++) {
                if (as[i] == 0 && bs[i] == 0) {
                    continue;
                } else if (as[i] == 0) {
                    bPow = bPow + Math.pow(bs[i], 2);
                } else if (bs[i] == 0) {
                    aPow = aPow + Math.pow(as[i], 2);
                } else {
                    sum = sum + as[i] * bs[i];
                    aPow = aPow + Math.pow(as[i], 2);
                    bPow = bPow + Math.pow(bs[i], 2);
                }

            }
            return new Double[]{sum, aPow, bPow};
        }*/

        @Override
        public Float[] call() throws Exception {
            float sum = 0;
            float aPow = 0;
            float bPow = 0;
            for (int i = beg; i <= end && i < as.length && i < bs.length; i++) {
                if (as[i] == 0 && bs[i] == 0) {
                    continue;
                } else if (as[i] == 0) {
                    bPow = bPow + multi(bs[i], bs[i]);
                } else if (bs[i] == 0) {
                    aPow = aPow + multi(as[i], as[i]);
                } else {
                    sum = sum + multi(as[i], bs[i]);
                    aPow = aPow + multi(as[i], as[i]);
                    bPow = bPow + multi(bs[i], bs[i]);
                }

            }
            return new Float[]{sum, aPow, bPow};
        }

        private float multi(double ad, double bd) {
            if (ad == 0 || bd == 0) {
                return 0;
            }
            long a = (long) (ad * 100000000);
            long b = (long) (bd * 100000000);
            int i = 0;
            long res = 0;
            while (b != 0) {
                if ((b & 1) == 1) {
                    res += (a << i);
                    b = b >> 1;
                    ++i;
                } else {
                    b = b >> 1;
                    ++i;
                }
            }
            return res / 100000000F;
        }
    }

    private float similar(float[] seedArr, float[] inArr) {
        float sumNum = 0;
        float sqrSeed = 0;
        float sqrIn = 0;
        for (int i = 0; i < seedArr.length && i < inArr.length; i++) {
            float a = seedArr[i];
            float b = inArr[i];
            sumNum = sumNum + a * b;
            sqrSeed = sqrSeed + a * a;
            sqrIn = sqrIn + b * b;
        }
        sqrSeed = (float) Math.sqrt(sqrSeed);
        sqrIn = (float) Math.sqrt(sqrIn);
        return sumNum / (sqrSeed * sqrIn);
    }

    private float similarCus(float[] seedArr, float[] inArr) {
        float sumNum = 0;
        float sqrSeed = 0;
        float sqrIn = 0;
        for (int i = 0; i < seedArr.length && i < inArr.length; i++) {
            float a = seedArr[i];
            float b = inArr[i];
            sumNum = sumNum + multi(a, b);
            sqrSeed = sqrSeed + multi(a, a);
            sqrIn = sqrIn + multi(b, b);
        }
        sqrSeed = (float) Math.sqrt(sqrSeed);
        sqrIn = (float) Math.sqrt(sqrIn);
        return sumNum / (sqrSeed * sqrIn);
    }

    private float multi(float ad, float bd) {
        if (ad == 0 || bd == 0) {
            return 0;
        }
        long a = (long) (ad * 100000000);
        long b = (long) (bd * 100000000);
        int i = 0;
        long res = 0;
        while (b != 0) {
            if ((b & 1) == 1) {
                res += (a << i);
                b = b >> 1;
                ++i;
            } else {
                b = b >> 1;
                ++i;
            }
        }
        return res / 100000000F;
    }
}
