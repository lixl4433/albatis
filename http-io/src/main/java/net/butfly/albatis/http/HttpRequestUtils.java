package net.butfly.albatis.http;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import net.butfly.albacore.io.URISpec;

public class HttpRequestUtils {
	
	 public static String doPost(String url, String json, Map<String, String> header) {
	        CloseableHttpClient httpClient = null;
	        CloseableHttpResponse httpResponse = null;
	        String result = "";
	        // 创建httpClient实例
	        httpClient = HttpClients.createDefault();
	        // 创建httpPost远程连接实例
	        HttpPost httpPost = new HttpPost(url);
	        // 配置请求参数实例
	        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(35000)// 设置连接主机服务超时时间
	                .setConnectionRequestTimeout(35000)// 设置连接请求超时时间
	                .setSocketTimeout(60000)// 设置读取数据连接超时时间
	                .build();
	        // 为httpPost实例设置配置
	        httpPost.setConfig(requestConfig);
	        // 设置请求头
	        httpPost.setHeader("Content-Type", "application/json;charset=UTF-8");
	        header.entrySet().forEach(kv -> httpPost.setHeader(kv.getKey(), kv.getValue()));
	        httpPost.setEntity(new StringEntity(json, "UTF-8"));
	        try {
	            // httpClient对象执行post请求,并返回响应参数对象
	            httpResponse = httpClient.execute(httpPost);
	            // 从响应对象中获取响应内容
	            HttpEntity entity = httpResponse.getEntity();
	            result = EntityUtils.toString(entity);
	        } catch (ClientProtocolException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            // 关闭资源
	            if (null != httpResponse) {
	                try {
	                    httpResponse.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	            if (null != httpClient) {
	                try {
	                    httpClient.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	        return result;
	    }
}
