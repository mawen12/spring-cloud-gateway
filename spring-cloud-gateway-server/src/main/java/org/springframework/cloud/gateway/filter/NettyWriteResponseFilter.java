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

import java.util.List;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.lang.Nullable;
import org.springframework.web.server.ServerWebExchange;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.CLIENT_RESPONSE_CONN_ATTR;

/**
 * 使用Netty回写响应的过滤器，将之前发起请求的连接保存，并从中解析响应体，写入到响应中
 *
 * @author Spencer Gibb
 * @see NettyRoutingFilter
 */
public class NettyWriteResponseFilter implements GlobalFilter, Ordered {

	/**
	 * Order for write response filter.
	 */
	public static final int WRITE_RESPONSE_FILTER_ORDER = -1;

	private static final Log log = LogFactory.getLog(NettyWriteResponseFilter.class);

	/**
	 * 支持的流媒体类型
	 */
	private final List<MediaType> streamingMediaTypes;

	public NettyWriteResponseFilter(List<MediaType> streamingMediaTypes) {
		this.streamingMediaTypes = streamingMediaTypes;
	}

	@Override
	public int getOrder() {
		return WRITE_RESPONSE_FILTER_ORDER;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// NOTICE: nothing in "pre" filter stage as CLIENT_RESPONSE_CONN_ATTR is not added
		// until the NettyRoutingFilter is run
		// @formatter:off
		return chain.filter(exchange)
				.then(Mono.defer(() -> {
					// 读取属性 gatewayClientResponseConnection 获取之前请求的响应连接信息
					Connection connection = exchange.getAttribute(CLIENT_RESPONSE_CONN_ATTR);

					// 如果响应不存在，则直接返回
					if (connection == null) {
						return Mono.empty();
					}
					if (log.isTraceEnabled()) {
						log.trace("NettyWriteResponseFilter start inbound: " + connection.channel().id().asShortText() + ", outbound: " + exchange.getLogPrefix());
					}
					ServerHttpResponse response = exchange.getResponse();

					// TODO: needed?
					// 从连接中获取响应
					final Flux<DataBuffer> body = connection
							.inbound()
							.receive()
							.retain()
							.map(byteBuf -> wrap(byteBuf, response));

					// 获取响应头上标识的内容类型
					MediaType contentType = null;
					try {
						contentType = response.getHeaders().getContentType();
					}
					catch (Exception e) {
						if (log.isTraceEnabled()) {
							log.trace("invalid media type", e);
						}
					}

					// 对于支持的媒体类型，执行写并刷新；反之直接写入
					return (isStreamingMediaType(contentType)
							? response.writeAndFlushWith(body.map(Flux::just))
							: response.writeWith(body));
					// 取消时，清理连接
				})).doOnCancel(() -> cleanup(exchange))
				// 保存时清理连接
				.doOnError(throwable -> cleanup(exchange));
		// @formatter:on
	}

	protected DataBuffer wrap(ByteBuf byteBuf, ServerHttpResponse response) {
		DataBufferFactory bufferFactory = response.bufferFactory();
		if (bufferFactory instanceof NettyDataBufferFactory) {
			NettyDataBufferFactory factory = (NettyDataBufferFactory) bufferFactory;
			return factory.wrap(byteBuf);
		}
		// MockServerHttpResponse creates these
		else if (bufferFactory instanceof DefaultDataBufferFactory) {
			DataBuffer buffer = ((DefaultDataBufferFactory) bufferFactory).allocateBuffer(byteBuf.readableBytes());
			buffer.write(byteBuf.nioBuffer());
			byteBuf.release();
			return buffer;
		}
		throw new IllegalArgumentException("Unkown DataBufferFactory type " + bufferFactory.getClass());
	}

	/**
	 * 关闭ServerWebExchange下gatewayClientResponseConnection连接
	 *
	 * @param exchange
	 */
	private void cleanup(ServerWebExchange exchange) {
		Connection connection = exchange.getAttribute(CLIENT_RESPONSE_CONN_ATTR);
		if (connection != null && connection.channel().isActive()) {
			connection.dispose();
		}
	}

	// TODO: use framework if possible
	/**
	 * 检查该过滤器是否支持的内容类型
	 *
	 * @param contentType
	 * @return
	 */
	private boolean isStreamingMediaType(@Nullable MediaType contentType) {
		if (contentType != null) {
			for (int i = 0; i < streamingMediaTypes.size(); i++) {
				if (streamingMediaTypes.get(i).isCompatibleWith(contentType)) {
					return true;
				}
			}
		}
		return false;
	}

}
