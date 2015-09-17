package com.agilor.distribute.consistenthash;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hash implements HashFunction {


	public Integer hash(String key) {
		// TODO Auto-generated method stub
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.reset();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		byte[] keyBytes = null;
		try {
			keyBytes = key.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		md5.update(keyBytes);
		
		byte[] hash = md5.digest();
		
		
//		 for(int h=0;h<4;h++) {  
//             //对于每四个字节，组成一个long值数值，做为这个虚拟节点的在环中的惟一key  
//               Long k = ((long)(digest[3+h*4]&0xFF) << 24)  
//                   | ((long)(digest[2+h*4]&0xFF) << 16)  
//                   | ((long)(digest[1+h*4]&0xFF) << 8)  
//                   | (digest[h*4]&0xFF); 
//               Long a1 = (long)(digest[3+h*4]&0xFF) << 24;
//               Long a2 = (long)(digest[2+h*4]&0xFF) << 16;
//               Long a3 = (long)(digest[1+h*4]&0xFF) << 8;
//               Long a4 = (long)(digest[0+h*4]&0xFF);
//           } 
		
//		int value = new BigInteger(digest).intValue();
		
        int node = 0;
        for (int i = 0; i < Math.min(4, hash.length); i++)
            node |= (0x000000ff & (int)hash[i]) << (3-i)*8;
//        assert (0xff00000000000000L & node) == 0;

        // Since we don't use the mac address, the spec says that multicast
        // bit (least significant bit of the first octet of the node ID) must be 1.
        return node;
	}

}
