package net.butfly.albatis.bcp.utils;

import net.butfly.albacore.utils.logger.Logger;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Md5校验工具类
 */
public class MD5Util {
	private static final Logger logger = Logger.getLogger(MD5Util.class);

	private static final char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public static void main(String[] args) {
		// 此处我测试的是我本机jdk源码文件的MD5值
		String filePath = "C:\\Users\\zhuqh\\Desktop\\b\\zip\\999-330000-1553766553-00791-zhk_MYSQL_HGRK-1-0.bcp";
		String md5Hashcode2 = MD5Util.getFileMD5(new File(filePath));
		String md5Hashcode = MD5Util.getFileMd5(filePath);// 4ed37133f4afa24825f69b6f1bebadac
		logger.debug("MD5Util2计算文件md5值为：" + md5Hashcode2);
		logger.debug("MD5Util2计算文件md5值的长度为：" + md5Hashcode2.length());
	}

	/**
	 * Get MD5 of a file (lower case)
	 *
	 * @return empty string if I/O error when get MD5
	 */
	public static String getFileMD5(File file) {

		FileInputStream in = null;
		try {
			in = new FileInputStream(file);
			FileChannel ch = in.getChannel();
			return MD5(ch.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
		} catch (FileNotFoundException e) {
			return "";
		} catch (IOException e) {
			return "";
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					// 关闭流产生的错误一般都可以忽略
				}
			}
		}

	}

	/**
	 * MD5校验字符串
	 *
	 * @param s String to be MD5
	 * @return 'null' if cannot get MessageDigest
	 */

	public static String getStringMD5(String s) {
		MessageDigest mdInst;
		try {
			// 获得MD5摘要算法的 MessageDigest 对象
			mdInst = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return "";
		}

		byte[] btInput = s.getBytes();
		// 使用指定的字节更新摘要
		mdInst.update(btInput);
		// 获得密文
		byte[] md = mdInst.digest();
		// 把密文转换成十六进制的字符串形式
		int length = md.length;
		char str[] = new char[length * 2];
		int k = 0;
		for (byte b : md) {
			str[k++] = hexDigits[b >>> 4 & 0xf];
			str[k++] = hexDigits[b & 0xf];
		}
		return new String(str);
	}

	@SuppressWarnings("unused")
	private static String getSubStr(String str, int subNu, char replace) {
		int length = str.length();
		if (length > subNu) {
			str = str.substring(length - subNu, length);
		} else if (length < subNu) {
			// NOTE: padding字符填充在字符串的右侧，和服务器的算法是一致的
			str += createPaddingString(subNu - length, replace);
		}
		return str;
	}

	private static String createPaddingString(int n, char pad) {
		if (n <= 0) { return ""; }

		char[] paddingArray = new char[n];
		Arrays.fill(paddingArray, pad);
		return new String(paddingArray);
	}

	/**
	 * 计算MD5校验
	 *
	 * @param buffer
	 * @return 空串，如果无法获得 MessageDigest实例
	 */

	private static String MD5(ByteBuffer buffer) {
		String s = "";
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(buffer);
			byte tmp[] = md.digest(); // MD5 的计算结果是一个 128 位的长整数，
			// 用字节表示就是 16 个字节
			char str[] = new char[16 * 2]; // 每个字节用 16 进制表示的话，使用两个字符，
			// 所以表示成 16 进制需要 32 个字符
			int k = 0; // 表示转换结果中对应的字符位置
			for (int i = 0; i < 16; i++) { // 从第一个字节开始，对 MD5 的每一个字节
				// 转换成 16 进制字符的转换
				byte byte0 = tmp[i]; // 取第 i 个字节
				str[k++] = hexDigits[byte0 >>> 4 & 0xf]; // 取字节中高 4 位的数字转换, >>>,
				// 逻辑右移，将符号位一起右移
				str[k++] = hexDigits[byte0 & 0xf]; // 取字节中低 4 位的数字转换
			}
			s = new String(str); // 换后的结果转换为字符串

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return s;
	}

	public static String getFileMd5(String path) {
		FileInputStream in = null;
		try {
			in = new FileInputStream(path);
			return DigestUtils.md5Hex(in);
		} catch (FileNotFoundException e) {
			return "";
		} catch (IOException e) {
			return "";
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}

	}

}
