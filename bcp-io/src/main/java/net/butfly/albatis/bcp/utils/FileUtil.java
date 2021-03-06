package net.butfly.albatis.bcp.utils;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Rmap;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static net.butfly.albatis.bcp.Props.CLEAN_TEMP_FILES;

public class FileUtil {

    private static final Logger LOGGER = Logger.getLogger(FileUtil.class);

    public static List<String> getFileNames(String fileSuffix, String tableName, String path) {
        File file = new File(path);
        File[] array = file.listFiles();
        if(null == array) LOGGER.warn("current user have no read auth of file");
        List<String> names = new ArrayList<>();
        for (File file1 : Objects.requireNonNull(array)) {
            if (file1.isFile()) {
                String[] strs = file1.getName().split("-");
                String[] strArray = file1.getName().split("\\.");
                int suffixIndex = strArray.length - 1;
                if (fileSuffix.equals(strArray[suffixIndex]) && tableName.equals(strs[4])) {
                    names.add(file1.getPath());
                }
            } else if (file1.isDirectory()) {
                getFileNames(fileSuffix, tableName, file1.getPath());
            }
        }
        return names;
    }


    public static List<Rmap> loadBcpData(List<String> fields, String separator, List<String> files, String table){
        List<InputStream> ins = new ArrayList<>();
        for(String f : files) try {
            ins.add(new FileInputStream(f));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        String line;
        List<Rmap> list = new ArrayList<>();
        for (InputStream in : ins){
            try (InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8); BufferedReader br = new BufferedReader(ir);) {
                while (null != (line = br.readLine())) {
                    list.add(integratedBcpData(line,fields,separator,table));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    private static Rmap integratedBcpData(String dataLine, List<String> fields, String separator,String table){
        if(null != dataLine && !dataLine.isEmpty()){
            String[] datas = dataLine.split(separator);
            Rmap m = new Rmap(new Qualifier(table));
            for(int i=0;i<fields.size();i++){
                if(i < datas.length) {
                    if(null!=datas[i] && !datas[i].isEmpty()){
                        m.put(fields.get(i),datas[i]);
                    }
                }
            }
            return m;
        }
        return null;
    }

    public static Path confirmDir(Path dir) {
        File file = dir.toFile();
        if (!file.exists() || !file.isDirectory()) file.mkdirs();
        return dir;
    }

    /**
     *  删除目录（文件夹）以及目录下的文件
     *  @param   sPath 被删除目录的文件路径
     * @return  目录删除成功返回true，否则返回false
     */
    public static boolean deleteDirectory(String sPath) {
        boolean flag;
        //如果sPath不以文件分隔符结尾，自动添加文件分隔符
        if (!sPath.endsWith(File.separator)) {
            sPath = sPath + File.separator;
        }
        File dirFile = new File(sPath);
        //如果dir对应的文件不存在，或者不是一个目录，则退出
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            return false;
        }
        flag = true;
        //删除文件夹下的所有文件(包括子目录)
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            //删除子文件
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag) break;
            } //删除子目录
            else {
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag) break;
            }
        }
        if (!flag) return false;
        //删除当前目录
        if (dirFile.delete()) {
            return true;
        } else {
            return false;
        }
    }

    public static void deleteZip(String path) {
        if(!CLEAN_TEMP_FILES) return;
        File file = new File(path);
        File temp = null;
        File[] fileList = file.listFiles();
        assert null != fileList;
        for (int i = 0; i < fileList.length; i++) {
            temp = fileList[i];
            if (temp.getName().endsWith("zip")) {
               temp.delete();
            }
        }
    }

    /**
     * 删除单个文件
     *
     * @param sPath 被删除文件的文件名
     * @return 单个文件删除成功返回true，否则返回false
     */
    public static boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            file.delete();
            flag = true;
        }
        return flag;
    }

}
