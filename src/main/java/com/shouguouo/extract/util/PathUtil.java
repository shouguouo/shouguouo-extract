package com.shouguouo.extract.util;

/**
 * @author shouguouo~
 * @date 2020/9/1 - 13:39
 */
public class PathUtil {

    protected static final String OS = System.getProperty("os.name").toLowerCase();

    public static boolean isWindows(){
        return OS.contains("windows");
    }

    public static String getServicePath() {
        String jarPath = PathUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (isWindows()) {
            if (jarPath.contains(".jar")) {
                jarPath = jarPath.substring(jarPath.indexOf("/") + 1, jarPath.lastIndexOf("/"));
            } else {
                jarPath = jarPath.substring(jarPath.indexOf("/") + 1);
            }
        } else{
            jarPath = jarPath.substring(0, jarPath.lastIndexOf("/"));
        }
        return jarPath;
    }
}
