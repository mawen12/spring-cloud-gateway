/*
 * Copyright 2013-2020 the original author or authors.
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

import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.CompletionContext;
import org.springframework.cloud.client.loadbalancer.DefaultRequest;
import org.springframework.cloud.client.loadbalancer.LoadBalancerLifecycle;
import org.springframework.cloud.client.loadbalancer.LoadBalancerLifecycleValidator;
import org.springframework.cloud.client.loadbalancer.LoadBalancerProperties;
import org.springframework.cloud.client.loadbalancer.LoadBalancerUriTools;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.RequestData;
import org.springframework.cloud.client.loadbalancer.RequestDataContext;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.client.loadbalancer.ResponseData;
import org.springframework.cloud.gateway.config.GatewayLoadBalancerProperties;
import org.springframework.cloud.gateway.support.DelegatingServiceInstance;
import org.springframework.cloud.gateway.support.NotFoundException;
import org.springframework.cloud.loadbalancer.core.ReactorLoadBalancer;
import org.springframework.cloud.loadbalancer.core.ReactorServiceInstanceLoadBalancer;
import org.springframework.cloud.loadbalancer.support.LoadBalancerClientFactory;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_LOADBALANCER_RESPONSE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_SCHEME_PREFIX_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.addOriginalRequestUrl;

/**
 * 基于反应式Spring Cloud LoadBalancer路由请求的{@link GlobalFilter}实现.
 *
 * <p>根据用户提供的路径，以及配置的gatewaySchemePrefix，如果使用lb，
 * 则代表使用负载均衡前往注册中心查找并选择实例，用于更新目标请求路径。
 *
 * <p>具体格式为：lb://serviceName
 * <ul>
 *     <li>lb代表使用LoadBalancer，但是仅仅这样不可以，还需要引入spring-cloud-starter-loadbalancer，因为它能够跟</li>
 * </ul>
 *
 * @author Spencer Gibb
 * @author Tim Ysewyn
 * @author Olga Maciaszek-Sharma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ReactiveLoadBalancerClientFilter implements GlobalFilter, Ordered {

	private static final Log log = LogFactory.getLog(ReactiveLoadBalancerClientFilter.class);

	/**
	 * 过滤器顺序
	 */
	public static final int LOAD_BALANCER_CLIENT_FILTER_ORDER = 10150;

	/**
	 * 负载均衡客户端工厂
	 */
	private final LoadBalancerClientFactory clientFactory;

	/**
	 * 网关负载均衡属性，属性前缀为: spring.cloud.gateway.loadbalancer
	 */
	private final GatewayLoadBalancerProperties properties;

	/**
	 * @deprecated in favour of
	 * {@link ReactiveLoadBalancerClientFilter#ReactiveLoadBalancerClientFilter(LoadBalancerClientFactory, GatewayLoadBalancerProperties)}
	 */
	@Deprecated
	public ReactiveLoadBalancerClientFilter(LoadBalancerClientFactory clientFactory, GatewayLoadBalancerProperties properties, LoadBalancerProperties loadBalancerProperties) {
		this.clientFactory = clientFactory;
		this.properties = properties;
	}

	public ReactiveLoadBalancerClientFilter(LoadBalancerClientFactory clientFactory, GatewayLoadBalancerProperties properties) {
		this.clientFactory = clientFactory;
		this.properties = properties;
	}

	@Override
	public int getOrder() {
		return LOAD_BALANCER_CLIENT_FILTER_ORDER;
	}

	/**
	 * 检查目标请求路径是否需要使用负载均衡
	 *
	 * @param exchange the current server exchange
	 * @param chain provides a way to delegate to the next filter
	 * @return
	 */
	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 读取属性 gatewayRequestUrl
		URI url = exchange.getAttribute(GATEWAY_REQUEST_URL_ATTR);
		// 读取属性 gatewaySchemePrefix
		String schemePrefix = exchange.getAttribute(GATEWAY_SCHEME_PREFIX_ATTR);
		// 如果url为空，或者没有以lb作为协议，则代表不需要走负载均衡，也不会使用注册中心查找服务
		if (url == null || (!"lb".equals(url.getScheme()) && !"lb".equals(schemePrefix))) {
			return chain.filter(exchange);
		}
		// 保留原始url，写入到 gatewayOriginalRequestUrl
		addOriginalRequestUrl(exchange, url);

		if (log.isTraceEnabled()) {
			// 打印过滤器和url
			log.trace(ReactiveLoadBalancerClientFilter.class.getSimpleName() + " url before: " + url);
		}

		// 获取请求路径，
		URI requestUri = exchange.getAttribute(GATEWAY_REQUEST_URL_ATTR);
		// 获取服务Id
		String serviceId = requestUri.getHost();
		// 获取负载均衡生命周期函数
		Set<LoadBalancerLifecycle> supportedLifecycleProcessors = LoadBalancerLifecycleValidator.getSupportedLifecycleProcessors(clientFactory.getInstances(serviceId, LoadBalancerLifecycle.class), RequestDataContext.class, ResponseData.class, ServiceInstance.class);
		// 构造默认请求
		DefaultRequest<RequestDataContext> lbRequest = new DefaultRequest<>(new RequestDataContext(new RequestData(exchange.getRequest(), exchange.getAttributes()), getHint(serviceId)));

		return choose(lbRequest, serviceId, supportedLifecycleProcessors).doOnNext(response -> {

			if (!response.hasServer()) {
				supportedLifecycleProcessors.forEach(lifecycle -> lifecycle
					.onComplete(new CompletionContext<>(CompletionContext.Status.DISCARD, lbRequest, response)));
				throw NotFoundException.create(properties.isUse404(), "Unable to find instance for " + url.getHost());
			}

			ServiceInstance retrievedInstance = response.getServer();

			URI uri = exchange.getRequest().getURI();

			// if the `lb:<scheme>` mechanism was used, use `<scheme>` as the default,
			// if the loadbalancer doesn't provide one.
			String overrideScheme = retrievedInstance.isSecure() ? "https" : "http";
			if (schemePrefix != null) {
				overrideScheme = url.getScheme();
			}

			DelegatingServiceInstance serviceInstance = new DelegatingServiceInstance(retrievedInstance, overrideScheme);

			URI requestUrl = reconstructURI(serviceInstance, uri);

			if (log.isTraceEnabled()) {
				log.trace("LoadBalancerClientFilter url chosen: " + requestUrl);
			}
			exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, requestUrl);
			exchange.getAttributes().put(GATEWAY_LOADBALANCER_RESPONSE_ATTR, response);
			supportedLifecycleProcessors.forEach(lifecycle -> lifecycle.onStartRequest(lbRequest, response));
		})
			.then(chain.filter(exchange))
			.doOnError(throwable -> supportedLifecycleProcessors.forEach(lifecycle -> lifecycle
				.onComplete(new CompletionContext<ResponseData, ServiceInstance, RequestDataContext>(
						CompletionContext.Status.FAILED, throwable, lbRequest,
						exchange.getAttribute(GATEWAY_LOADBALANCER_RESPONSE_ATTR)))))
			.doOnSuccess(aVoid -> supportedLifecycleProcessors.forEach(lifecycle -> lifecycle
				.onComplete(new CompletionContext<ResponseData, ServiceInstance, RequestDataContext>(
						CompletionContext.Status.SUCCESS, lbRequest,
						exchange.getAttribute(GATEWAY_LOADBALANCER_RESPONSE_ATTR),
						new ResponseData(exchange.getResponse(),
								new RequestData(exchange.getRequest(), exchange.getAttributes()))))));
	}

	/**
	 * 重新构造URI，例如原始URI为lb:posts/posts/1，而实际的服务实例为127.0.0.1:8081，经过该方法处理后的结果为127.0.0.1:8081/posts/1
	 *
	 * @param serviceInstance
	 * @param original
	 * @return
	 */
	protected URI reconstructURI(ServiceInstance serviceInstance, URI original) {
		return LoadBalancerUriTools.reconstructURI(serviceInstance, original);
	}

	/**
	 * 选择一个服务实例并返回
	 *
	 * @param lbRequest 负载均衡请求
	 * @param serviceId 服务Id
	 * @param supportedLifecycleProcessors 生命周期函数
	 * @return 返回一个服务实例
	 */
	private Mono<Response<ServiceInstance>> choose(Request<RequestDataContext> lbRequest, String serviceId, Set<LoadBalancerLifecycle> supportedLifecycleProcessors) {
		// 获取该服务的负载均衡器
		ReactorLoadBalancer<ServiceInstance> loadBalancer = this.clientFactory.getInstance(serviceId, ReactorServiceInstanceLoadBalancer.class);
		if (loadBalancer == null) {
			// 负载均衡器为空，抛出未找到异常
			throw new NotFoundException("No loadbalancer available for " + serviceId);
		}
		// 触发生命周期开始函数
		supportedLifecycleProcessors.forEach(lifecycle -> lifecycle.onStart(lbRequest));
		// 根据请求选择一个服务实例
		return loadBalancer.choose(lbRequest);
	}

	private String getHint(String serviceId) {
		LoadBalancerProperties loadBalancerProperties = clientFactory.getProperties(serviceId);
		Map<String, String> hints = loadBalancerProperties.getHint();
		String defaultHint = hints.getOrDefault("default", "default");
		String hintPropertyValue = hints.get(serviceId);
		return hintPropertyValue != null ? hintPropertyValue : defaultHint;
	}

}
