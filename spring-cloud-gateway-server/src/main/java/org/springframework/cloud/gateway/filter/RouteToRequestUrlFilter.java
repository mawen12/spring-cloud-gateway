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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.route.Route;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_SCHEME_PREFIX_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.containsEncodedParts;

/**
 * 解析路由的uri，将其协议、主机和端口拼接原始URL的全局过滤器
 *
 * <p>需要注意的是，其处理协议、主机和端口，其他的数据不处理
 *
 * <p>示例如下：
 * Route配置：
 * <pre>
 *     spring:
 *     	cloud:
 *     	  gateway:
 *     	  	routes:
 *     	  	  - id: posts-route
 *     	  	    uri: https://httpbin.org:443
 *     	  	    predicates:
 *     	  	      - Path=/posts/**
 * </pre>
 * 原始请求路径：http:8080/posts/1
 * 处理后的请求路径：https://httpbin.org:443/posts/1
 *
 * @author Spencer Gibb
 */
public class RouteToRequestUrlFilter implements GlobalFilter, Ordered {

	/**
	 * Order of Route to URL.
	 */
	public static final int ROUTE_TO_URL_FILTER_ORDER = 10000;

	private static final Log log = LogFactory.getLog(RouteToRequestUrlFilter.class);

	private static final String SCHEME_REGEX = "[a-zA-Z]([a-zA-Z]|\\d|\\+|\\.|-)*:.*";

	static final Pattern schemePattern = Pattern.compile(SCHEME_REGEX);

	/* for testing */
	static boolean hasAnotherScheme(URI uri) {
		return schemePattern.matcher(uri.getSchemeSpecificPart()).matches() && uri.getHost() == null && uri.getRawPath() == null;
	}

	@Override
	public int getOrder() {
		return ROUTE_TO_URL_FILTER_ORDER;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 读取属性 gatewayRoute，获取路由信息
		Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);
		if (route == null) {
			// 没有路由信息，直接执行下一个交换器
			return chain.filter(exchange);
		}
		log.trace("RouteToRequestUrlFilter start");
		// 获取路由请求的URI
		URI uri = exchange.getRequest().getURI();
		// 对齐进行编码处理
		boolean encoded = containsEncodedParts(uri);
		// 获取路由URI
		URI routeUri = route.getUri();

		if (hasAnotherScheme(routeUri)) {
			// 处理特殊的URL，将该协议保存到 gatewaySchemePrefix
			exchange.getAttributes().put(GATEWAY_SCHEME_PREFIX_ATTR, routeUri.getScheme());
			// 使用协议特殊部分替换原来的路由URI
			routeUri = URI.create(routeUri.getSchemeSpecificPart());
		}

		if ("lb".equalsIgnoreCase(routeUri.getScheme()) && routeUri.getHost() == null) {
			// 负载均衡的URI应该具有主机名，如果主机名没有，那就代表主机名非法
			// 负载均衡的格式:lb://posts，其主机名就是服务名
			throw new IllegalStateException("Invalid host: " + routeUri.toString());
		}

		// 使用路由URI中的协议、主机和端口替换原始的部分
		URI mergedUrl = UriComponentsBuilder.fromUri(uri)
				// .uri(routeUri)
				.scheme(routeUri.getScheme())
				.host(routeUri.getHost())
				.port(routeUri.getPort())
				.build(encoded)
				.toUri();
		// 保存到属性 gatewayRequestUrl
		exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, mergedUrl);
		return chain.filter(exchange);
	}

}
