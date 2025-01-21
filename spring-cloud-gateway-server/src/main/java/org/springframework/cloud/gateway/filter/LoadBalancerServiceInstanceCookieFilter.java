/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.gateway.filter;

import java.util.ArrayList;
import java.util.List;

import reactor.core.publisher.Mono;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.LoadBalancerProperties;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.client.loadbalancer.reactive.ReactiveLoadBalancer;
import org.springframework.core.Ordered;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.filter.ReactiveLoadBalancerClientFilter.LOAD_BALANCER_CLIENT_FILTER_ORDER;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR;

/**
 * 允许将{@link ReactiveLoadBalancerClientFilter}选择的{@link ServiceInstance#getInstanceId()}保存到请求头的Cookie中的全局过滤器。
 *
 * <p>该过滤器依赖于{@link ReactiveLoadBalancerClientFilter}，因此需要在其之后执行
 * <p>该过滤器会修改请求头，按需向其写入Cookie: InstanceId
 *
 * @author Olga Maciaszek-Sharma
 * @since 3.0.2
 */
public class LoadBalancerServiceInstanceCookieFilter implements GlobalFilter, Ordered {

	private LoadBalancerProperties loadBalancerProperties;

	private ReactiveLoadBalancer.Factory<ServiceInstance> loadBalancerClientFactory;

	LoadBalancerServiceInstanceCookieFilter(LoadBalancerProperties loadBalancerProperties) {
		this.loadBalancerProperties = loadBalancerProperties;
	}

	public LoadBalancerServiceInstanceCookieFilter(
			ReactiveLoadBalancer.Factory<ServiceInstance> loadBalancerClientFactory) {
		this.loadBalancerClientFactory = loadBalancerClientFactory;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 获取通过ReactiveLoadBalancerClientFilter选择的服务实例响应
		Response<ServiceInstance> serviceInstanceResponse = exchange.getAttribute(GATEWAY_LOADBALANCER_RESPONSE_ATTR);
		// 不处理响应为空或者服务实例不存在的场景
		if (serviceInstanceResponse == null || !serviceInstanceResponse.hasServer()) {
			return chain.filter(exchange);
		}
		// 获取服务对应的负载均衡配置，如果没有指定，则使用默认的
		LoadBalancerProperties properties = loadBalancerClientFactory != null
				? loadBalancerClientFactory.getProperties(serviceInstanceResponse.getServer().getServiceId())
				: loadBalancerProperties;
		// 不处理未开启粘连会话的服务
		if (!properties.getStickySession().isAddServiceInstanceCookie()) {
			return chain.filter(exchange);
		}
		// 获取服务实例的cookie名称
		String instanceIdCookieName = properties.getStickySession().getInstanceIdCookieName();
		// 不处理cookie名称未设置的服务
		if (!StringUtils.hasText(instanceIdCookieName)) {
			return chain.filter(exchange);
		}

		// 突变请求头并构建新的服务Web交换
		ServerWebExchange newExchange = exchange.mutate().request(exchange.getRequest().mutate().headers((headers) -> {
			// 获取Cookie请求头
			List<String> cookieHeaders = new ArrayList<>(headers.getOrEmpty(HttpHeaders.COOKIE));
			// 构造配置的Cookie名称保存服务实例ID
			String serviceInstanceCookie = new HttpCookie(instanceIdCookieName, serviceInstanceResponse.getServer().getInstanceId()).toString();
			cookieHeaders.add(serviceInstanceCookie);
			// 写入请求头
			headers.put(HttpHeaders.COOKIE, cookieHeaders);
		}).build()).build();
		return chain.filter(newExchange);
	}

	@Override
	public int getOrder() {
		// 该过滤器位于客户端负载均衡过滤器之后
		return LOAD_BALANCER_CLIENT_FILTER_ORDER + 1;
	}

}
