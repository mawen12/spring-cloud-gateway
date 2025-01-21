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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.event.EnableBodyCachingEvent;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CACHED_REQUEST_BODY_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;

/**
 * 适配缓存体的全局过滤器
 */
public class AdaptCachedBodyGlobalFilter implements GlobalFilter, Ordered, ApplicationListener<EnableBodyCachingEvent> {

	private ConcurrentMap<String, Boolean> routesToCache = new ConcurrentHashMap<>();

	@Override
	public void onApplicationEvent(EnableBodyCachingEvent event) {
		this.routesToCache.putIfAbsent(event.getRouteId(), true);
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 当ServerWebExchange无法变异时，使用缓存的ServerHttpRequest。例如在读取主体的谓词期间，但仍然需要缓存
		// 获取属性 cachedServerHttpRequestDecorator
		ServerHttpRequest cachedRequest = exchange.getAttributeOrDefault(CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR, null);
		if (cachedRequest != null) {
			// 移除属性 cachedServerHttpRequestDecorator
			exchange.getAttributes().remove(CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR);
			// 对请求进行变异并构建
			return chain.filter(exchange.mutate().request(cachedRequest).build());
		}

		// 读取属性 cachedRequestBody
		DataBuffer body = exchange.getAttributeOrDefault(CACHED_REQUEST_BODY_ATTR, null);
		// 读取属性 gatewayRoute
		Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);

		if (body != null || !this.routesToCache.containsKey(route.getId())) {
			// 在请求体不为空，或者缓存中不包含该路由的时候，跳过并执行下一个过滤器
			return chain.filter(exchange);
		}

		// 缓存请求体
		return ServerWebExchangeUtils.cacheRequestBody(exchange, (serverHttpRequest) -> {
			// 如果请求对象相同则不进行变异和构建
			if (serverHttpRequest == exchange.getRequest()) {
				return chain.filter(exchange);
			}
			// 对请求进行变异并构建
			return chain.filter(exchange.mutate().request(serverHttpRequest).build());
		});
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE + 1000;
	}

}
