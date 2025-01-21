/*
 * Copyright 2013-2024 the original author or authors.
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

import java.net.URI;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;

import static org.springframework.cloud.gateway.support.GatewayToStringStyler.filterToStringCreator;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.containsEncodedParts;

/**
 * 增加请求参数的网关过滤器工厂
 *
 * @author Spencer Gibb
 */
public class AddRequestParameterGatewayFilterFactory extends AbstractNameValueGatewayFilterFactory {

	/**
	 * 创建 {@code AddRequestParameterGatewayFilter}
	 *
	 * @param config
	 * @return
	 */
	@Override
	public GatewayFilter apply(NameValueConfig config) {
		return new GatewayFilter() {
			@Override
			public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
				// 获取请求的URI
				URI uri = exchange.getRequest().getURI();
				StringBuilder query = new StringBuilder();
				// 获取原始的查询
				String originalQuery = uri.getRawQuery();

				if (StringUtils.hasText(originalQuery)) {
					// 拼接原始查询
					query.append(originalQuery);
					if (originalQuery.charAt(originalQuery.length() - 1) != '&') {
						// 如果末尾没有&，则拼接&
						query.append('&');
					}
				}

				// 使用uriTemplateVariables对值进行构造，拼接
				String value = ServerWebExchangeUtils.expand(exchange, config.getValue());
				// TODO urlencode?
				query.append(config.getName());
				query.append('=');
				query.append(value);

				// 检查是否已编码
				boolean encoded = containsEncodedParts(uri);
				try {
					// 使用新的查询构造URI
					URI newUri = UriComponentsBuilder.fromUri(uri)
						.replaceQuery(query.toString())
						.build(encoded)
						.toUri();

					// 突变并构造新的服务http请求
					ServerHttpRequest request = exchange.getRequest().mutate().uri(newUri).build();

					// 突变并构造新的服务web交换，用于执行后续过滤器
					return chain.filter(exchange.mutate().request(request).build());
				}
				catch (RuntimeException ex) {
					throw new IllegalStateException("Invalid URI query: \"" + query.toString() + "\"");
				}
			}

			@Override
			public String toString() {
				return filterToStringCreator(AddRequestParameterGatewayFilterFactory.this).append(config.getName(), config.getValue()).toString();
			}
		};
	}

}
