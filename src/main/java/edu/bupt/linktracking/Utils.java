package edu.bupt.linktracking;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class Utils {
    public static long toLong(String str, long defaultValue) {
        if (str == null) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException var4) {
                return defaultValue;
            }
        }
    }

    public static String MD5(String key) {
        char hexDigits[] = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
        };
        try {
            byte[] btInput = key.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean isClientProcess() {
        String port = System.getProperty("server.port", "8080");
        if (Constants.CLIENT_PROCESS_PORT1.equals(port) ||
                Constants.CLIENT_PROCESS_PORT2.equals(port)) {
            return true;
        }
        return false;
    }

    public static boolean isBackendProcess() {
        String port = System.getProperty("server.port", "8080");
        if (Constants.BACKEND_PROCESS_PORT1.equals(port)) {
            return true;
        }
        return false;
    }

    /**
     * 以大端模式将int转成byte[]
     */
    public static byte[] intToBytesBig(int value) {
        byte[] src = new byte[4];
        src[0] = (byte) ((value >> 24) & 0xFF);
        src[1] = (byte) ((value >> 16) & 0xFF);
        src[2] = (byte) ((value >> 8) & 0xFF);
        src[3] = (byte) (value & 0xFF);
        return src;
    }

    /**
     * 以大端模式将byte[]转成int
     */
    public static int bytesToIntBig(byte[] src, int offset) {
        int value;
        value = ((src[offset] & 0xFF) << 24)
                | ((src[offset + 1] & 0xFF) << 16)
                | ((src[offset + 2] & 0xFF) << 8)
                | (src[offset + 3] & 0xFF);
        return value;
    }

    /**
     * 报文格式: 4(len of str's bytes) + 4(integer) + [str's bytes]
     *
     * @param os      OutputStream
     * @param integer int
     * @param str     string
     */
    public static void sendIntAndStringFromStream(OutputStream os, int integer, String str) throws IOException {
        byte[] dataBytes = str.getBytes(StandardCharsets.UTF_8);
        byte[] lenBytes = intToBytesBig(dataBytes.length);
        byte[] batchPosBytes = intToBytesBig(integer);
        os.write(lenBytes);
        os.write(batchPosBytes);
        os.write(dataBytes);
        os.flush();
    }

    /**
     * 报文格式: 4(len of str's bytes) + 4(integer) + [str's bytes]
     *
     * @param is             InputStream
     * @param lenAndIntBytes must be 8
     * @param dataBytes      may be enlarged, so return
     * @param result         save (Integer, String), return null when illegal end
     * @return dataCache
     */
    public static byte[] receiveIntAndStringFromStream(InputStream is, byte[] lenAndIntBytes, byte[] dataBytes, Pair<Integer, String> result) throws IOException {
        int readNum = 0;
        while (readNum < 8) {
            int currRead = is.read(lenAndIntBytes, readNum, 8 - readNum);
            if (currRead == -1) {
                if (readNum == 0) { // exit normally
                    return dataBytes;
                }
                return null;
            }
            readNum += currRead;
        }

        int len = bytesToIntBig(lenAndIntBytes, 0);
        int num = bytesToIntBig(lenAndIntBytes, 4);

        int dataBytesLen = dataBytes.length;
        while (len > dataBytesLen) {
            dataBytesLen *= 2;
        }
        if (dataBytesLen != dataBytes.length) {
            dataBytes = new byte[dataBytesLen];
        }

        readNum = 0;
        while (readNum < len) {
            int currRead = is.read(dataBytes, readNum, len - readNum);
            if (currRead == -1) {
                return null;
            }
            readNum += currRead;
        }

        String str = new String(dataBytes, 0, len, StandardCharsets.UTF_8);
        result.first = num;
        result.second = str;

        return dataBytes;
    }
}
