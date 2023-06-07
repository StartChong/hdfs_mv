package demo;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description : TODO
 * @author: lichong
 * @date: 2022/10/19
 * @version: 1.0
 */
public class HdfsMV {

    private final Logger logger = LoggerFactory.getLogger(HdfsMV.class);

    // 处理成功文件个数
    private AtomicInteger successFiles = new AtomicInteger(0);
    // 处理失败文件个数
    private AtomicInteger failedFiles = new AtomicInteger(0);
    // 记录当次完成的最后分区
    private final String partitionTime = "hdfs_mv_time/lastRunHour.txt";
    // 读hdfs
    private final HadoopUtils readHadoop;
    // 写hdfs
    private final HadoopUtils writeHadoop;
    private String[] args;

    public HdfsMV(String[] params) {
        this.args = params;
        this.readHadoop = new HadoopUtils(params[0]);
        this.writeHadoop = new HadoopUtils(params[1]);
    }

    // 转移文件
    public void mvFile() throws Exception {
        // args[0]为原始父路径,args[1]为目标父路径
        if (args == null || args.length < 2 || args.length > 4) {
            logger.info("必须传入不少于2个,不大于4个的参数,各参数说明如下：");
            logger.info("第1个参数为原始父路径(需要转移的hdfs目录,不能为空!)");
            logger.info("第2个参数为目标父路径(转移目标hdfs目录,不能为空!)");
            logger.info("第3个参数为开启的线程核心数数量(默认为5,最终结果会根据文件数量调整,最大线程数为该数量的2倍,可以为空!)");
            logger.info("第4个参数为分区时间戳(默认读取上一次分区日,格式：yyyyMMdd,长度为8位,如：20221020,可以为空)");
            return;
        }
        // 默认线程数
        int nThreads = 5;
        if (args.length > 2 && StringUtils.isNotBlank(args[2])) {
            nThreads = Integer.parseInt(args[2]);
        }
        List<String> sourceFiles = getSourceFiles();
        if (CollectionUtils.isNotEmpty(sourceFiles)) {
            int size = sourceFiles.size();
            logger.info("获取路径成功！共需要转移" + size + "个文件，开始对目录和文件进行复制转移--------------------");
            // 如果需要处理的文件数小于核心线程数，则重新设置核心线程数为文件数
            if (size < nThreads) {
                nThreads = size;
            }
            // 最大线程数
            int maxPoolSize = nThreads * 2;
            // 线程池默认开启线程
            ExecutorService executor = new ThreadPoolExecutor(nThreads, maxPoolSize, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(size), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
            logger.info("=======线程池开启:核心线程数：" + nThreads + "/最大线程数：" + maxPoolSize + "======");
            long start = System.currentTimeMillis();
            //使用CountDownLatch确保所有子线程结束后再结束主线程
            CountDownLatch countDownLatch = new CountDownLatch(size);
            // 将List集合切片
            for (String sourceFile : sourceFiles) {
                // 让线程池执行工作
                executor.execute(new MvSourceToTargetThreading(sourceFile, args, countDownLatch));
            }
            try {
                countDownLatch.await();
                // 将最后的分区xxx/20221020/12/xxx.xx写入目录partition_time/lastRunHour.txt
                String[] lastPathArr = sourceFiles.get(sourceFiles.size() - 1).split("/");
                String lastPartitionTime = lastPathArr[lastPathArr.length - 3];
                FileUtils.saveAsFileWriter(partitionTime, lastPartitionTime);
                logger.info("本次执行最后的分区记录在目录:" + partitionTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                // 关闭线程池
                executor.shutdown();
                int i = 0;
                while (!executor.isTerminated() && i++ < 10) {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                }
                if (i >= 10) {
                    executor.shutdownNow();
                }
                logger.info("=======线程池关闭======");
                //关闭hdfs文件系统
                if (readHadoop.getFs() != null) {
                    try {
                        readHadoop.closeResource();
                        logger.info("=======readHadoop关闭======");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (writeHadoop.getFs() != null) {
                    try {
                        writeHadoop.closeResource();
                        logger.info("=======writeHadoop关闭======");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            long end = System.currentTimeMillis();
            logger.info("目录和文件复制转移完毕！用时：" + (end - start) + "毫秒 -》" + DateFormatUtils.format((end - start), "HH小时mm分ss秒", TimeZone.getTimeZone("GMT+0:00")));
            logger.info(String.format("总共转移文件%d个，成功%d个，失败%d个。", size, successFiles.get(), failedFiles.get()));
        } else {
            logger.info("获取路径文件数为0,程序结束！");
        }
    }

    // 获取原目标未完成的目录文件集
    private List<String> getSourceFiles() {
        // 先从输入的参数中获取,若不存在则从上次执行完毕后的目录开始,若也不存在则从当前日期获取
        String startRunDay;
        String nowRunDay = DateFormatUtils.format(new Date(), "yyyyMMdd");
        if (args.length == 4 && StringUtils.isNotBlank(args[3])) {
            if (args[3].length() != 8) {
                logger.info("第4个参数为分区时间戳(默认读取上一次分区日,格式：yyyyMMdd,长度为8位,如：20221020,可以为空)");
                return null;
            }
            startRunDay = args[3];
        } else {
            startRunDay = FileUtils.readToString(partitionTime);
            if (StringUtils.isBlank(startRunDay)) {
                startRunDay = nowRunDay;
            }
        }
        logger.info("将从分区：" + startRunDay + "到当天:" + nowRunDay + "依次进行任务！");
        List<String> days = DateUtils.getBetweenDate(startRunDay, nowRunDay);
        List<String> sourceFiles = Lists.newArrayList();
        for (String day : days) {
            String path1 = args[0] + "/" + day;
            List<String> dayFiles;
            logger.info("开始获取路径:" + path1 + " 下的所有文件和目录--------------------");
            try {
                if (readHadoop.isDirectory(path1)) {
                    dayFiles = readHadoop.getChildrenPath(path1, null, null);
                } else {
                    logger.info("路径1:" + path1 + " 文件或目录不存在,自动跳过！");
                    continue;
                }
            } catch (IOException ex) {
                logger.info("路径1:" + path1 + " 文件或目录不存在,自动跳过！");
                continue;
            }
            if (CollectionUtils.isNotEmpty(dayFiles)) {
                List<String> targetFiles = Lists.newArrayList();
                // 去除已经存在的目标
                String path2 = args[1] + "/" + day;
                try {
                    if (writeHadoop.isDirectory(path2)) {
                        targetFiles = writeHadoop.getChildrenPath(path2, null, null);
                    } else {
                        // 目标目录不存在就创建
                        writeHadoop.getFs().mkdirs(new Path(path2));
                    }
                } catch (IOException e) {
                    try {
                        writeHadoop.getFs().mkdirs(new Path(path2));
                    } catch (IOException ex) {
                        logger.info("路径2:" + path1 + " 文件或目录创建失败！");
                    }
                }
                if (CollectionUtils.isNotEmpty(targetFiles)) {
                    List<String> rmFiles = Lists.newArrayList();
                    targetFiles.forEach(t -> rmFiles.add(t.replace(args[1], args[0])));
                    dayFiles.removeAll(rmFiles);
                }
                // 将不存在的目录添加到任务中
                sourceFiles.addAll(dayFiles);
            }
        }
        return sourceFiles;
    }

    // 将原始目录文件转移
    public class MvSourceToTargetThreading implements Runnable {

        private CountDownLatch countDownLatch;

        private String[] args;

        private String sourceFile;

        MvSourceToTargetThreading(String sourceFile, String[] args, CountDownLatch countDownLatch) {
            this.sourceFile = sourceFile;
            this.args = args;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            Path currentPath = new Path(sourceFile);
            // 目标路径  /sss/xxx/tt.txt
            String target = args[1] + currentPath.toString().replace(args[0], "");
            Path targetPath = new Path(target);
            // 目标临时路径
            String pre = "_" + UUID.randomUUID();
            String lsTarget = target.substring(0, target.lastIndexOf("/") + 1) + pre;
            Path lsTargetPath = new Path(lsTarget);
            try {
                FileUtil.copy(readHadoop.getFs(), currentPath, writeHadoop.getFs(), lsTargetPath, false, readHadoop.getConf());
                writeHadoop.getFs().rename(lsTargetPath, targetPath);
                successFiles.getAndIncrement();
                logger.info("" + Thread.currentThread().getName() + ":" + currentPath + "---》" + targetPath + "  状态：成功");
            } catch (Exception e) {
                failedFiles.getAndIncrement();
                logger.info("" + Thread.currentThread().getName() + ":" + currentPath + "---》" + targetPath + "  状态：失败");
                try {
                    writeHadoop.getFs().delete(lsTargetPath, true);
                } catch (IOException ex) {
                    logger.info("" + Thread.currentThread().getName() + ":临时文件" + lsTargetPath + "删除失败！");
                }
            } finally {
                countDownLatch.countDown();
            }
        }

    }

}
