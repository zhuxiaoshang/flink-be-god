package org.apache.flink.redis.api.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * @author: zhushang
 * @create: 2021-04-06 14:45
 */
public class DESUtils {

    private static final String KEY = "key"; // 加密、解密的密钥、8的整数位
    private static final String DES = "DES"; // 双向加密算法

    /**
     * 加密后使用 base64 编码
     *
     * @param content 原始内容
     * @return
     */
    public static String encryptFromBase64(String content) {
        if (null == content) {
            return content;
        }
        byte[] encryptRaw = encrypt(content);
        assert encryptRaw != null;
        return new String(Base64.getEncoder().encode(encryptRaw));
    }

    /**
     * 从 base64 编码后的字符串解密
     *
     * @param content
     * @return
     */
    public static String decryptFromBase64(String content) {
        if (null == content) {
            return content;
        }
        byte[] decoded = Base64.getDecoder().decode(content);
        return decrypt(decoded);
    }

    /**
     * 加密
     *
     * @param content 待加密内容
     * @return
     */
    private static byte[] encrypt(String content) {
        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
            SecretKey securekey = keyFactory.generateSecret(desKey);
            Cipher cipher = Cipher.getInstance(DES);
            cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
            byte[] result = cipher.doFinal(content.getBytes());
            return result;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 解密
     *
     * @param content 待解密内容
     * @return
     */
    private static String decrypt(byte[] content) {
        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(KEY.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
            SecretKey securekey = keyFactory.generateSecret(desKey);
            Cipher cipher = Cipher.getInstance(DES);
            cipher.init(Cipher.DECRYPT_MODE, securekey, random);
            byte[] result = cipher.doFinal(content);
            return new String(result);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(encryptFromBase64("123456"));
    }
}
