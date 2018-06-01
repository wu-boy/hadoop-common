package com.wu.hadoop.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS工具类
 * @author wusq
 * @date 2018-05-23
 */
public class HadoopFileUtils {

    /**
     * 向文件追加数据
     * @param conf
     * @param uri
     * @param content
     */
    public static void append(Configuration conf, String uri, String content){
        FileSystem fs = null;
        FSDataOutputStream output = null;
        try{
            fs = FileSystem.get(conf);
            output = fs.append(new Path(uri));
            byte[] bytes = content.getBytes();
            output.write(bytes, 0, bytes.length);
            output.flush();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(output, fs);
        }
    }

    /**
     * 复制一个目录下的所有文件
     * @param conf
     * @param from
     * @param to
     */
    public static void copyDir(Configuration conf, String from, String to){
        FileSystem fs = null;
        String pathSplit = "/";
        try{
            fs = FileSystem.get(conf);

            String[] array = from.split(pathSplit);
            String lastName = array[array.length-1];
            String toPath = to + pathSplit + lastName;

            Path fromPath = new Path(from);
            if(fs.isDirectory(fromPath)){

                fs.mkdirs(new Path(toPath));
                FileStatus[] status = fs.listStatus(fromPath);
                if(status != null){
                    for(FileStatus s:status){
                        copyDir(conf, s.getPath().toString(), toPath);
                    }
                }
            }else{
                fs.mkdirs(new Path(to));
                copyFile(conf, from, toPath);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(fs);
        }
    }

    /**
     * 复制文件
     * @param conf
     * @param from
     * @param to
     */
    public static void copyFile(Configuration conf, String from, String to){

        FileSystem fs = null;
        FSDataInputStream input = null;
        FSDataOutputStream output = null;
        try{
            fs = FileSystem.get(conf);
            // 建立输入流
            input = fs.open(new Path(from));
            // 建立输出流
            output = fs.create(new Path(to));
            // 两个流对接
            byte[] b = new byte[1024];
            int hasRead = 0;
            while((hasRead=input.read(b))>0){
                output.write(b, 0, hasRead);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(output, input, fs);
        }

    }

    /**
     * 创建文件并写入数据
     * @param conf
     * @param uri
     * @param content
     */
    public static void createFile(Configuration conf, String uri, String content){
        FileSystem fs = null;
        FSDataOutputStream output = null;
        try{
            fs = FileSystem.get(conf);
            output = fs.create(new Path(uri));
            byte[] bytes = content.getBytes();
            output.write(bytes, 0, bytes.length);
            output.flush();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(output, fs);
        }
    }

    /**
     * 删除目录或文件
     * @param conf
     * @param uris
     * @return
     */
    public static boolean delete(Configuration conf, String... uris){
        boolean result = false;
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            for(String uri:uris){
                Path path = new Path(uri);
                if(fs.exists(path)){
                    fs.delete(path, true);
                }
            }
            result = true;
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(fs);
        }
        return result;
    }

    /**
     * 判断目录或文件是否存在
     * @param conf
     * @param uri
     * @return
     */
    public static boolean exist(Configuration conf, String uri){
        boolean result = Boolean.FALSE;
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            Path path = new Path(uri);
            if(fs.exists(path)){
                result = Boolean.TRUE;
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(fs);
        }
        return result;
    }

    /**
     * 返回uri目录下的所有path，仅返回文件的path
     * @param conf
     * @param uri
     * @return
     */
    public static List<Path> listFiles(Configuration conf, String uri){
        List<Path> result = new ArrayList<Path>();
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            Path path = new Path(uri);
            FileStatus[] status = fs.listStatus(path);
            for(FileStatus s:status){
                if(s.isFile()){
                    result.add(s.getPath());
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(fs);
        }
        return result;
    }

    /**
     * 返回path目录下的所有目录
     * @param conf
     * @param uri
     * @return
     */
    public static List<Path> listDirectories(Configuration conf, String uri){
        List<Path> result = new ArrayList<Path>();
        FileSystem fs = null;
        try{
            fs = FileSystem.get(conf);
            Path path = new Path(uri);
            FileStatus[] status = fs.listStatus(path);
            for(FileStatus s:status){
                if(s.isDirectory()){
                    result.add(s.getPath());
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            close(fs);
        }
        return result;
    }

    /**
     * 创建目录
     * @param conf
     * @param folder
     */
    public static void mkdirs(Configuration conf, String folder){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(folder);
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            close(fs);
        }
    }

    /**
     * 读取文件
     * @param conf
     * @param uri
     * @return List<String>
     */
    public static List<String> readLines(Configuration conf, String uri){
        List<String> result = new ArrayList<>();
        FileSystem fs = null;
        FSDataInputStream is = null;
        try{
            fs = FileSystem.get(conf);
            is = fs.open(new Path(uri));
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(line + "");
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(is);
            close(fs);
        }
        return result;
    }

    public static void rename(Configuration conf, Path src, Path tar){
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            fs.rename(src, tar);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            close(fs);
        }
    }

    /**
     * 关闭资源
     * @param fs 文件系统
     */
    static void close(FileSystem fs){
        close(null, null, fs);
    }

    /**
     * 关闭资源
     * @param output 输出流
     * @param fs 文件系统
     */
    static void close(FSDataOutputStream output, FileSystem fs){
        close(output, null, fs);
    }

    /**
     * 关闭资源
     * @param output 输出流
     * @param input 输入流
     * @param fs 文件系统
     */
    static void close(FSDataOutputStream output, FSDataInputStream input, FileSystem fs){
        try {
            if(output != null){
                output.close();
            }
            if(input != null){
                input.close();
            }
            if(fs != null){
                fs.closeAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
