import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 请在此类中完成解决方案，实现process完成数据的处理逻辑。
 *
 * @author gaosiyu
 * @date 2020年11月13日 上午9:35:32
 */
public class Solution {

//    private static final ExecutorService executor = new ThreadPoolExecutor(4, 4,
//            10, TimeUnit.MILLISECONDS,
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
        BufferedReader seedFileReader = new BufferedReader(new FileReader(seedFile));
        String line = null;
        while ((line = seedFileReader.readLine()) != null) {
            String[] cells = line.split(",");
            String key = cells[0];
            float[] vector = new float[cells.length - 1];
            for (int i = 1; i < cells.length; i++) {
                vector[i - 1] = Float.parseFloat(cells[i]);
            }
            seedLines.add(new Pair<>(key, vector));
        }
        BufferedReader allReader = new BufferedReader(new FileReader(allFile));
        LinkedList<String> link = new LinkedList<>();
        ArrayList<Float> array = new ArrayList<>();
        while ((line = allReader.readLine()) != null) {
            String[] allArr = line.split(",");
            String key = allArr[0];
            float[] vector = new float[allArr.length - 1];
            for (int i = 1; i < allArr.length; i++) {
                vector[i - 1] = Float.parseFloat(allArr[i]);
            }
//            float maxScore = 0;
//            String maxId = null;
//            List<Future<Float>> fList = new ArrayList<>();

//            for (int i = 0; i < seedLines.size(); i++) {
/*                float d = similarParallel(seedLines.get(i).getValue(), vector);
                float d = similarCus(seedLines.get(i).getValue(), vector);*/
//                float d = similar(seedLines.get(i).getValue(), vector);
//                if (d > maxScore) {
//                    maxScore = d;
//                    maxId = key;
//                }
//                Future<Float> f = executor.submit(new VectorMultiCallable(seedLines.get(i).getValue(), vector));
//                fList.add(f);
//            }
            /*Optional<CompletableFuture<Pair>> pf = seedLines.stream().map(seedVector -> CompletableFuture.supplyAsync(() -> {
                return new Pair(seedVector.getKey(), similar(seedVector.getValue(), vector));
            }, executor)).max(new Comparator<CompletableFuture<Pair>>() {
                @Override
                public int compare(CompletableFuture<Pair> o1, CompletableFuture<Pair> o2) {
                    try {
                        float f1 = (float) o1.get().getValue();
                        float f2 = (float) o2.get().getValue();
                        if (f2 > f1) {
                            return 1;
                        } else if (f2 < f1) {
                            return -1;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    return 0;
                }
            });

            if (pf.isPresent()) {
                float d = (float) pf.get().get().getValue();
                if (d > maxScore) {
                    maxScore = d;
                    maxId = key;
                }
            }
            if (maxId != null && !maxId.isEmpty()) {
                addQueue(link, array, maxId, maxScore, outputCount);
            }*/
            Float f= similarGroup(seedLines, vector);
            addQueue(link, array, key, f, outputCount);
        }
        MainFrame.addSet(link);

/*        String[] result = new String[]{"0X123", "0X124", "0X253", "0X16C"};

        //请通过此方法输出答案,多次调用会追加记录
        MainFrame.addSet(result);*/
    }

    private void addQueue(LinkedList<String> queue, ArrayList<Float> sorted, String id, float score, int count) {
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
            if (i != sorted.size()) {
                float tmp = sorted.get(i);
                sorted.set(i, f);
                f = tmp;
            } else {
                sorted.add(f);
                break;
            }
        }
    }

    private Float similarGroup(List<Pair<String, float[]>> seedLines, float[] vector)
            throws ExecutionException, InterruptedException {
        int step = seedLines.size() / 4;
        List<Future<Pair<String, Float>>> l = new ArrayList<>();
        for (int i = 0; i + step - 1 < seedLines.size(); i += step) {
            Future<Pair<String, Float>> f = executor.submit(new VectorSimilar(seedLines, i, i + step - 1, vector));
            l.add(f);
        }
        float maxScore = 0;
        for (Future<Pair<String, Float>> fp : l) {
            float f = fp.get().getValue();
            if (f > maxScore) {
                maxScore = f;
            }
        }
        return maxScore;
    }


    private class VectorSimilar implements Callable<Pair<String, Float>> {
        private List<Pair<String, float[]>> board;
        float[] vector;
        private int beg;
        private int end;

        public VectorSimilar(List<Pair<String, float[]>> board, int beg, int end, float[] vector) {
            this.board = board;
            this.vector = vector;
            this.beg = beg;
            this.end = end;
        }

        @Override
        public Pair<String, Float> call() throws Exception {
            float maxScore = 0;
            String maxId = null;
            for (int i = beg; i <= end; i++) {
                Pair<String, float[]> p = board.get(i);
                float d = similar(p.getValue(), vector);
                if (d > maxScore) {
                    maxScore = d;
                    maxId = p.getKey();
                }
            }
            return new Pair<>(maxId, maxScore);
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
//        return sumNum / (float)( Math.sqrt(sqrSeed)* Math.sqrt(sqrIn));
            return sumNum / sqrt(sqrSeed * sqrIn);
        }

        private float sqrt(float f) {
            final float xhalf = f * 0.5F;
            float y = Float.intBitsToFloat(0x5f375a86 - (Float.floatToIntBits(f) >> 1));
            y = y * (1.5F - (xhalf * y * y));
            y = y * (1.5F - (xhalf * y * y));
            return f * y;
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
//        return sumNum / (float)( Math.sqrt(sqrSeed)* Math.sqrt(sqrIn));
        return sumNum / sqrt(sqrSeed * sqrIn);
    }

    private float sqrt(float f) {
        final float xhalf = f * 0.5F;
        float y = Float.intBitsToFloat(0x5f375a86 - (Float.floatToIntBits(f) >> 1));
        y = y * (1.5F - (xhalf * y * y));
        y = y * (1.5F - (xhalf * y * y));
        return f * y;
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
        return sum / (float) (Math.sqrt(aPow) * Math.sqrt(bPow));
    }


    private class VectorMultiCallable implements Callable<Float> {

        private float[] as;
        private float[] bs;

        public VectorMultiCallable(float[] as, float[] bs) {
            this.as = as;
            this.bs = bs;
        }

        @Override
        public Float call() throws Exception {
            return similar(as, bs);
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
//        return sumNum / (float)( Math.sqrt(sqrSeed)* Math.sqrt(sqrIn));
            return sumNum / (sqrt(sqrSeed) * sqrIn);
        }

        private float sqrt(float f) {
            float xhalf = f * 0.5F;
            float y = Float.intBitsToFloat(0x5f375a86 - (Float.floatToIntBits(f) >> 1));
            y = y * (1.5F - (xhalf * y * y));
            y = y * (1.5F - (xhalf * y * y));
            return f * y;
        }
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
}
