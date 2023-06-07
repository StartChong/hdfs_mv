package demo;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @Description : TODO
 * @author: lichong
 * @date: 2022/10/21
 * @version: 1.0
 */
public class FileUtils {

    // 读取文件内容并返回
    public static String readToString(String fileName) {
        String encoding = "UTF-8";
        //new File对象
        File file = new File(fileName);
        //获取文件长度
        long fileLength = file.length();
        //获取同长度的字节数组
        byte[] fileContent = new byte[(int) fileLength];
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            in.read(fileContent);
            return new String(fileContent, encoding);
        } catch (Exception e) {
            //e.printStackTrace();
            return null;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //向文件里写入内容
    public static void saveAsFileWriter(String path, String content) {
        File file = new File(path);
        //如果目录不存在 创建目录
        try {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileWriter fileWriter = null;
        try {
            // true表示不覆盖原来的内容，而是加到文件的后面。若要覆盖原来的内容，直接省略这个参数就好
            fileWriter = new FileWriter(path, false);
            fileWriter.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.flush();
                    fileWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
