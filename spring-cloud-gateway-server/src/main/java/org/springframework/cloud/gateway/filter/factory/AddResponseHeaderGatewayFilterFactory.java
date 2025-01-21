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

package org.springframework.cloud.gateway.filter.factory;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.GatewayToStringStyler.filterToStringCreator;

/**
 * 增加响应头的网关过滤器工厂
 *
 * @author Spencer Gibb
 */
public class AddResponseHeaderGatewayFilterFactory extends AbstractNameValueGatewayFilterFactory {

	/**
	 * 创建{@code AddResponseHeaderGatewayFilter}
	 *
	 * @param config
	 * @return
	 */
	@Override
	public GatewayFilter apply(NameValueConfig config) {
		return new GatewayFilter() {
			@Override
			public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
				// 执行后续过滤器，并在之后执行添加响应头的操作
				return chain.filter(exchange).then(Mono.fromRunnable(() -> addHeader(exchange, config)));
			}

			@Override
			public String toString() {
				return filterToStringCreator(AddResponseHeaderGatewayFilterFactory.this).append(config.getName(), config.getValue()).toString();
			}
		};
	}

	void addHeader(ServerWebExchange exchange, NameValueConfig config) {
		// 使用uriTemplateVariables对值进行格式化
		final String value = ServerWebExchangeUtils.expand(exchange, config.getValue());
		// 获取响应头
		HttpHeaders headers = exchange.getResponse().getHeaders();
		if (!exchange.getResponse().isCommitted()) {
			// 对于尚未提交的响应，添加头信息
			headers.add(config.getName(), value);
		}
	}

}
