/*
*
* Copyright 2013 Netflix, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.netflix.ribbon.examples.restclient;

import java.net.URI;

import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.loadbalancer.ZoneAwareLoadBalancer;
import com.netflix.niws.client.http.RestClient;

public class SampleApp {
	public static void main(String[] args) throws Exception {
	    // 从资源文件加载配置
        ConfigurationManager.loadPropertiesFromResources("sample-client.properties");  // 1

        System.out.println(ConfigurationManager.getConfigInstance().getProperty("sample-client.ribbon.listOfServers"));
        // 根据服务名称从client工厂获取一个client
        RestClient client = (RestClient) ClientFactory.getNamedClient("sample-client");  // 2
        // 构建一个http请求
        HttpRequest request = HttpRequest.newBuilder().uri(new URI("/")).build(); // 3

        for (int i = 0; i < 20; i++)  {
            // 用这个服务的client发起负载均衡的请求并获取结果
        	HttpResponse response = client.executeWithLoadBalancer(request); // 4
        	System.out.println("Status code for " + response.getRequestedURI() + "  :" + response.getStatus());
        }

        @SuppressWarnings("rawtypes")
        ZoneAwareLoadBalancer lb = (ZoneAwareLoadBalancer) client.getLoadBalancer();

        System.out.println(lb.getLoadBalancerStats());
//        ConfigurationManager.getConfigInstance().setProperty(
//        		"sample-client.ribbon.listOfServers", "www.linkedin.com:80,www.google.com:80"); // 5

        ConfigurationManager.getConfigInstance().setProperty(
                "sample-client.ribbon.listOfServers", "www.baidu.com,www.taobao.com"); // 5

        System.out.println("changing servers ...");

        Thread.sleep(3000); // 6
        for (int i = 0; i < 20; i++)  {
            HttpResponse response = null;
            try {
        	    response = client.executeWithLoadBalancer(request);
        	    System.out.println("Status code for " + response.getRequestedURI() + "  : " + response.getStatus());
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
        System.out.println(lb.getLoadBalancerStats()); // 7
	}
}
