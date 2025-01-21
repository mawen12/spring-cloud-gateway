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

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.route.Route;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.isAlreadyRouted;

/**
 * 转发路径过滤器，当{@link Route}中URI协议为forward时，使用{@code Route#getUri()#getPath()}来作为新请求的路径
 *
 * @author Ryan Baxter
 */
public class ForwardPathFilter implements GlobalFilter, Ordered {

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 获取 org.springframework.cloud.gateway.support.ServerWebExchangeUtils.gatewayRoute
		Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);
		// 获取路由的URI
		URI routeUri = route.getUri();
		// 获取URI的协议
		String scheme = routeUri.getScheme();
		// 对于已经路由，或非forward协议，直接执行下一个过滤器
		if (isAlreadyRouted(exchange) || !"forward".equals(scheme)) {
			return chain.filter(exchange);
		}
		// 进行突变并使用路由中URI的路径构建新的请求
		exchange = exchange.mutate().request(exchange.getRequest().mutate().path(routeUri.getPath()).build()).build();
		return chain.filter(exchange);
	}

	@Override
	public int getOrder() {
		return 0;
	}

}
