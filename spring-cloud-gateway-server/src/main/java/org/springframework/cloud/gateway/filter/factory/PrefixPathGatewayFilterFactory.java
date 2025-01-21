/*
 * Copyright 2013-2022 the original author or authors.
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriTemplate;

import static org.springframework.cloud.gateway.support.GatewayToStringStyler.filterToStringCreator;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ALREADY_PREFIXED_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.addOriginalRequestUrl;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.getUriTemplateVariables;

/**
 * 基于前缀路径的网关过滤器工厂
 *
 * @author Spencer Gibb
 */
public class PrefixPathGatewayFilterFactory extends AbstractGatewayFilterFactory<PrefixPathGatewayFilterFactory.Config> {

	/**
	 * 前缀关键词
	 */
	public static final String PREFIX_KEY = "prefix";

	private static final Log log = LogFactory.getLog(PrefixPathGatewayFilterFactory.class);

	public PrefixPathGatewayFilterFactory() {
		super(Config.class);
	}

	@Override
	public List<String> shortcutFieldOrder() {
		return Arrays.asList(PREFIX_KEY);
	}

	/**
	 * 创建{@code PrefixPathGatewayFilter}
	 *
	 * @param config
	 * @return
	 */
	@Override
	public GatewayFilter apply(Config config) {
		return new GatewayFilter() {
			// 构造Uri模版
			final UriTemplate uriTemplate = new UriTemplate(config.prefix);

			@Override
			public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
				// 获取属性 org.springframework.cloud.gateway.support.ServerWebExchangeUtils.gatewayAlreadyPrefixed，用于检查是否已经进行过前缀追加的工作
				boolean alreadyPrefixed = exchange.getAttributeOrDefault(GATEWAY_ALREADY_PREFIXED_ATTR, false);
				if (alreadyPrefixed) {
					// 已经执行过，不再重复执行
					return chain.filter(exchange);
				}
				// 更新属性 org.springframework.cloud.gateway.support.ServerWebExchangeUtils.gatewayAlreadyPrefixed 为true
				exchange.getAttributes().put(GATEWAY_ALREADY_PREFIXED_ATTR, true);

				// 读取请求
				ServerHttpRequest req = exchange.getRequest();
				// 保存原始的请求路径到 gatewayOriginalRequestUrl
				addOriginalRequestUrl(exchange, req.getURI());

				// 读取属性 uriTemplateVariables
				Map<String, String> uriVariables = getUriTemplateVariables(exchange);
				// 扩展该属性
				URI uri = uriTemplate.expand(uriVariables);

				// 重新构造路径，格式为{prefixPath}+{rawPath}
				String newPath = uri.getRawPath() + req.getURI().getRawPath();
				// 更新属性 gatewayRequestUrl
				exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, uri);
				// 使用该路径构造新的请求
				ServerHttpRequest request = req.mutate().path(newPath).build();

				if (log.isTraceEnabled()) {
					log.trace("Prefixed URI with: " + config.prefix + " -> " + request.getURI());
				}

				// 突变并构建新的ServerWebExchange
				return chain.filter(exchange.mutate().request(request).build());
			}

			@Override
			public String toString() {
				return filterToStringCreator(PrefixPathGatewayFilterFactory.this).append("prefix", config.getPrefix()).toString();
			}
		};
	}

	public static class Config {

		private String prefix;

		public String getPrefix() {
			return prefix;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}

	}

}
