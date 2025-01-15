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
import java.time.Duration;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.config.HttpClientProperties;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.Type;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.TimeoutException;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.filterRequest;
import static org.springframework.cloud.gateway.support.RouteMetadataUtils.CONNECT_TIMEOUT_ATTR;
import static org.springframework.cloud.gateway.support.RouteMetadataUtils.RESPONSE_TIMEOUT_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_CONN_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_HEADER_NAMES;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.PRESERVE_HOST_HEADER_ATTRIBUTE;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.isAlreadyRouted;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.setAlreadyRouted;

/**
 * 基于Netty的路由过滤器，最低优先级。使用Netty发起服务请求，处理请求头、接受响应、处理响应头。
 *
 * @author Spencer Gibb
 * @author Biju Kunjummen
 * @see NettyWriteResponseFilter
 */
public class NettyRoutingFilter implements GlobalFilter, Ordered {

	/**
	 * The order of the NettyRoutingFilter. See {@link Ordered#LOWEST_PRECEDENCE}.
	 */
	public static final int ORDER = Ordered.LOWEST_PRECEDENCE;

	private static final Log log = LogFactory.getLog(NettyRoutingFilter.class);

	/**
	 * 进行http请求的客户端
	 */
	private final HttpClient httpClient;

	/**
	 * 提供http请求头过滤器的生产者
	 */
	private final ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider;

	/**
	 * http请求相关的属性
	 */
	private final HttpClientProperties properties;

	/**
	 * 不要直接使用该类，而是通过{@link #getHeadersFilters()}来获取
	 */
	private volatile List<HttpHeadersFilter> headersFilters;

	public NettyRoutingFilter(HttpClient httpClient, ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider, HttpClientProperties properties) {
		this.httpClient = httpClient;
		this.headersFiltersProvider = headersFiltersProvider;
		this.properties = properties;
	}

	public List<HttpHeadersFilter> getHeadersFilters() {
		if (headersFilters == null) {
			// 如果该值为空，则进行初始化
			headersFilters = headersFiltersProvider.getIfAvailable();
		}
		return headersFilters;
	}

	@Override
	public int getOrder() {
		return ORDER;
	}

	@Override
	@SuppressWarnings("Duplicates")
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 获取属性 gatewayRequestUrl
		URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);

		// 获取协议
		String scheme = requestUrl.getScheme();
		// 如果之前已经有路由触发了，或者非http和https协议的请求，则执行下一个过滤器
		if (isAlreadyRouted(exchange) || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
			return chain.filter(exchange);
		}
		// 更新路由触发成功的属性标识
		setAlreadyRouted(exchange);

		// 获取http请求
		ServerHttpRequest request = exchange.getRequest();
		// 解析http方法
		final HttpMethod method = HttpMethod.valueOf(request.getMethod().name());
		// 将请求路径转换为ASCII字符串
		final String url = requestUrl.toASCIIString();
		// 将http头过滤器应用到请求头上
		HttpHeaders filtered = filterRequest(getHeadersFilters(), exchange);
		// 使用默认的http请求头保存过滤后的请求头
		final DefaultHttpHeaders httpHeaders = new DefaultHttpHeaders();
		filtered.forEach(httpHeaders::set);

		// 获取属性 preserveHostHeader
		boolean preserveHost = exchange.getAttributeOrDefault(PRESERVE_HOST_HEADER_ATTRIBUTE, false);
		// 获取属性 gatewayRoute
		Route route = exchange.getAttribute(GATEWAY_ROUTE_ATTR);

		Flux<HttpClientResponse> responseFlux = getHttpClientMono(route, exchange)
				.flatMapMany(httpClient -> httpClient.headers(headers -> {
							// 写入请求头
							headers.add(httpHeaders);
							// 要么在下面设置，要么之后由Netty设置
							headers.remove(HttpHeaders.HOST);

							if (preserveHost) {
								// 如果保留主机，则将主机信息写入请求头
								String host = request.getHeaders().getFirst(HttpHeaders.HOST);
								headers.add(HttpHeaders.HOST, host);
							}
						})
						// 设置请求方法
						.request(method)
						// 设置请求路径
						.uri(url)
						// 发送请求
						.send((req, nettyOutbound) -> {
							if (log.isTraceEnabled()) {
								nettyOutbound.withConnection(connection -> log.trace("outbound route: " + connection.channel().id().asShortText() + ", inbound: " + exchange.getLogPrefix()));
							}
							return nettyOutbound.send(request.getBody().map(this::getByteBuf));
						})
						// 回写响应
						.responseConnection((res, connection) -> {

							// Defer committing the response until all route filters have run
							// Put client response as ServerWebExchange attribute and write
							// response later NettyWriteResponseFilter

							// 写入响应
							exchange.getAttributes().put(CLIENT_RESPONSE_ATTR, res);
							// 写入响应的连接地址
							exchange.getAttributes().put(CLIENT_RESPONSE_CONN_ATTR, connection);
							// 获取响应
							ServerHttpResponse response = exchange.getResponse();
							// 构造用于存放响应头和状态的http头信息
							HttpHeaders headers = new HttpHeaders();
							// 将原始响应的请求头复制到新的http头
							res.responseHeaders().forEach(entry -> headers.add(entry.getKey(), entry.getValue()));

							// 获取响应请求头的内容类型
							String contentTypeValue = headers.getFirst(HttpHeaders.CONTENT_TYPE);
							if (StringUtils.hasLength(contentTypeValue)) {
								// 写入非空的内容类型
								exchange.getAttributes().put(ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR, contentTypeValue);
							}

							// 写入响应状态码
							setResponseStatus(res, response);

							// make sure headers filters run after setting status so it is
							// available in response
							// 将http头过滤器应用到响应头上
							HttpHeaders filteredResponseHeaders = HttpHeadersFilter.filter(getHeadersFilters(), headers, exchange, Type.RESPONSE);

							//
							if (!filteredResponseHeaders.containsKey(HttpHeaders.TRANSFER_ENCODING) && filteredResponseHeaders.containsKey(HttpHeaders.CONTENT_LENGTH)) {
								// 同时拥有Transfer-Encoding和Content-Length的标头是无效的。如果存在内容长度标头，则在响应中删除传输编码标头
								response.getHeaders().remove(HttpHeaders.TRANSFER_ENCODING);
							}

							// 写入响应头名称
							exchange.getAttributes().put(CLIENT_RESPONSE_HEADER_NAMES, filteredResponseHeaders.keySet());

							// 将过滤后的响应头覆盖原先的响应头
							response.getHeaders().addAll(filteredResponseHeaders);

							// 返回响应
							return Mono.just(res);
						}));

		// 获取响应超时时间
		Duration responseTimeout = getResponseTimeout(route);
		if (responseTimeout != null) {
			responseFlux = responseFlux
					// 超时设置
					.timeout(responseTimeout, Mono.defer(() -> Mono.error(new TimeoutException("Response took longer than timeout: " + responseTimeout))))
					// 异常场景
					.onErrorMap(TimeoutException.class, th -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, th.getMessage(), th));
		}

		return responseFlux.then(chain.filter(exchange));
	}

	protected ByteBuf getByteBuf(DataBuffer dataBuffer) {
		if (dataBuffer instanceof NettyDataBuffer) {
			NettyDataBuffer buffer = (NettyDataBuffer) dataBuffer;
			return buffer.getNativeBuffer();
		}
		// MockServerHttpResponse creates these
		else if (dataBuffer instanceof DefaultDataBuffer) {
			DefaultDataBuffer buffer = (DefaultDataBuffer) dataBuffer;
			return Unpooled.wrappedBuffer(buffer.getNativeBuffer());
		}
		throw new IllegalArgumentException("Unable to handle DataBuffer of type " + dataBuffer.getClass());
	}

	private void setResponseStatus(HttpClientResponse clientResponse, ServerHttpResponse response) {
		// 解析http响应状态码
		HttpStatus status = HttpStatus.resolve(clientResponse.status().code());
		if (status != null) {
			// 回写响应状态
			response.setStatusCode(status);
		} else {
			// 对于装饰器类型的响应代理，获取其原始的响应
			while (response instanceof ServerHttpResponseDecorator) {
				response = ((ServerHttpResponseDecorator) response).getDelegate();
			}
			if (response instanceof AbstractServerHttpResponse) {
				((AbstractServerHttpResponse) response).setRawStatusCode(clientResponse.status().code());
			} else {
				// TODO: log warning here, not throw error?
				throw new IllegalStateException("Unable to set status code " + clientResponse.status().code() + " on response of type " + response.getClass().getName());
			}
		}
	}

	/**
	 * Creates a new HttpClient with per route timeout configuration. Sub-classes that
	 * override, should call super.httpClient() if they want to honor the per route
	 * timeout configuration.
	 *
	 * @param route    the current route.
	 * @param exchange the current ServerWebExchange.
	 *
	 * @return the configured HttpClient.
	 */
	protected Mono<HttpClient> getHttpClientMono(Route route, ServerWebExchange exchange) {
		return Mono.just(getHttpClient(route, exchange));
	}

	/**
	 * Creates a new HttpClient with per route timeout configuration. Sub-classes that
	 * override, should call super.getHttpClient() if they want to honor the per route
	 * timeout configuration.
	 *
	 * @param route    the current route.
	 * @param exchange the current ServerWebExchange.
	 *
	 * @return the configured HttpClient.
	 */
	protected HttpClient getHttpClient(Route route, ServerWebExchange exchange) {
		Object connectTimeoutAttr = route.getMetadata().get(CONNECT_TIMEOUT_ATTR);
		if (connectTimeoutAttr != null) {
			Integer connectTimeout = getInteger(connectTimeoutAttr);
			return this.httpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
		}
		return httpClient;
	}

	static Integer getInteger(Object connectTimeoutAttr) {
		Integer connectTimeout;
		if (connectTimeoutAttr instanceof Integer) {
			connectTimeout = (Integer) connectTimeoutAttr;
		} else {
			connectTimeout = Integer.parseInt(connectTimeoutAttr.toString());
		}
		return connectTimeout;
	}

	private Duration getResponseTimeout(Route route) {
		try {
			if (route.getMetadata().containsKey(RESPONSE_TIMEOUT_ATTR)) {
				// 获取路由元信息中的response-timeout
				Long routeResponseTimeout = getLong(route.getMetadata().get(RESPONSE_TIMEOUT_ATTR));
				if (routeResponseTimeout != null && routeResponseTimeout >= 0) {
					return Duration.ofMillis(routeResponseTimeout);
				} else {
					return null;
				}
			}
		} catch (NumberFormatException e) {
			// ignore number format and use global default
		}

		// 如果路由中没有响应超时时间，则使用http客户端属性
		return properties.getResponseTimeout();
	}

	static Long getLong(Object responseTimeoutAttr) {
		Long responseTimeout = null;
		if (responseTimeoutAttr instanceof Number) {
			responseTimeout = ((Number) responseTimeoutAttr).longValue();
		} else if (responseTimeoutAttr != null) {
			responseTimeout = Long.parseLong(responseTimeoutAttr.toString());
		}
		return responseTimeout;
	}

}
