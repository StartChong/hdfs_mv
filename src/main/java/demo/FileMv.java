package demo;


/**
 * @Description : TODO
 * @author: lichong
 * @date: 2022/10/14
 * @version: 1.0
 */
public class FileMv {

    public static void main(String[] args) throws Exception {
        // 复制集群
        HdfsMV hdfsMV = new HdfsMV(args);
        hdfsMV.mvFile();
    }

}
