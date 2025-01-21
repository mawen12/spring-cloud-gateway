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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;

import static org.springframework.cloud.gateway.filter.headers.HttpHeadersFilter.filterRequest;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.PRESERVE_HOST_HEADER_ATTRIBUTE;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.containsEncodedParts;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.isAlreadyRouted;
import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.setAlreadyRouted;

/**
 * 基于Websocket的路由过滤器
 *
 * <p>该过滤器可能会修改请求协议
 * <p>该过滤器可能会发起websocket请求
 *
 * @see NettyRoutingFilter
 * @see WebClientHttpRoutingFilter
 *
 * @author Spencer Gibb
 * @author Nikita Konev
 */
public class WebsocketRoutingFilter implements GlobalFilter, Ordered {

	/**
	 * Sec-Websocket protocol.
	 */
	public static final String SEC_WEBSOCKET_PROTOCOL = "Sec-WebSocket-Protocol";

	private static final Log log = LogFactory.getLog(WebsocketRoutingFilter.class);

	/**
	 * 负责发起Websocket请求的客户端
	 */
	private final WebSocketClient webSocketClient;

	/**
	 * 负责处理websocket请求、响应的服务
	 */
	private final WebSocketService webSocketService;

	/**
	 * 提供请求头过滤器的对象
	 */
	private final ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider;

	// do not use this headersFilters directly, use getHeadersFilters() instead.
	private volatile List<HttpHeadersFilter> headersFilters;

	public WebsocketRoutingFilter(WebSocketClient webSocketClient, WebSocketService webSocketService, ObjectProvider<List<HttpHeadersFilter>> headersFiltersProvider) {
		this.webSocketClient = webSocketClient;
		this.webSocketService = webSocketService;
		this.headersFiltersProvider = headersFiltersProvider;
	}

	/* for testing */
	/**
	 * 将http协议转换为websocket协议
	 * <ul>
	 *     <li>http -> ws</li>
	 *     <li>https -> wss</li>
	 * </ul>
	 *
	 * @param scheme 请求协议
	 * @return
	 */
	static String convertHttpToWs(String scheme) {
		scheme = scheme.toLowerCase(Locale.ROOT);
		return "http".equals(scheme) ? "ws" : "https".equals(scheme) ? "wss" : scheme;
	}

	/**
	 * 在{@link NettyRoutingFilter}之前执行
	 */
	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE - 1;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 检查协议并按需更新协议和请求路径
		changeSchemeIfIsWebSocketUpgrade(exchange);
		// 获取请求路径
		URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);
		String scheme = requestUrl.getScheme();
		// 如果已经路由过和非websocket协议，不再进行路由
		if (isAlreadyRouted(exchange) || (!"ws".equals(scheme) && !"wss".equals(scheme))) {
			return chain.filter(exchange);
		}
		// 更新路由状态为已路由
		setAlreadyRouted(exchange);
		// 获取请求头
		HttpHeaders headers = exchange.getRequest().getHeaders();
		// 使用请求头过滤器对请求进行过滤器
		HttpHeaders filtered = filterRequest(getHeadersFilters(), exchange);
		// 获取协议
		List<String> protocols = getProtocols(headers);
		// 发起websocket请求
		return this.webSocketService.handleRequest(exchange, new ProxyWebSocketHandler(requestUrl, this.webSocketClient, filtered, protocols));
	}

	/* for testing */ List<String> getProtocols(HttpHeaders headers) {
		// 读取协议 Sec-WebSocket-Protocol
		List<String> protocols = headers.get(SEC_WEBSOCKET_PROTOCOL);
		if (protocols != null) {
			ArrayList<String> updatedProtocols = new ArrayList<>();
			for (int i = 0; i < protocols.size(); i++) {
				String protocol = protocols.get(i);
				// 对协议按,分隔
				updatedProtocols.addAll(Arrays.asList(StringUtils.tokenizeToStringArray(protocol, ",")));
			}
			protocols = updatedProtocols;
		}
		return protocols;
	}

	/* for testing */ List<HttpHeadersFilter> getHeadersFilters() {
		if (this.headersFilters == null) {
			this.headersFilters = this.headersFiltersProvider.getIfAvailable(ArrayList::new);

			// remove host header unless specifically asked not to
			headersFilters.add((headers, exchange) -> {
				HttpHeaders filtered = new HttpHeaders();
				filtered.addAll(headers);
				filtered.remove(HttpHeaders.HOST);
				boolean preserveHost = exchange.getAttributeOrDefault(PRESERVE_HOST_HEADER_ATTRIBUTE, false);
				if (preserveHost) {
					String host = exchange.getRequest().getHeaders().getFirst(HttpHeaders.HOST);
					filtered.add(HttpHeaders.HOST, host);
				}
				return filtered;
			});

			headersFilters.add((headers, exchange) -> {
				HttpHeaders filtered = new HttpHeaders();
				for (Map.Entry<String, List<String>> entry : headers.headerSet()) {
					if (!entry.getKey().toLowerCase(Locale.ROOT).startsWith("sec-websocket")) {
						filtered.addAll(entry.getKey(), entry.getValue());
					}
				}
				return filtered;
			});
		}

		return this.headersFilters;
	}

	static void changeSchemeIfIsWebSocketUpgrade(ServerWebExchange exchange) {
		// 获取请求的URL
		URI requestUrl = exchange.getRequiredAttribute(GATEWAY_REQUEST_URL_ATTR);
		// 获取请求协议
		String scheme = requestUrl.getScheme().toLowerCase(Locale.ROOT);
		// 获取请求头 Upgrade
		String upgrade = exchange.getRequest().getHeaders().getUpgrade();
		// 如果原始协议是http或https，则修改为ws或wss
		if ("WebSocket".equalsIgnoreCase(upgrade) && ("http".equals(scheme) || "https".equals(scheme))) {
			// 转换为websocket协议
			String wsScheme = convertHttpToWs(scheme);
			boolean encoded = containsEncodedParts(requestUrl);
			// 使用新的协议重新构造请求路径
			URI wsRequestUrl = UriComponentsBuilder.fromUri(requestUrl).scheme(wsScheme).build(encoded).toUri();
			// 保存属性到 gatewayRequestUrl
			exchange.getAttributes().put(GATEWAY_REQUEST_URL_ATTR, wsRequestUrl);
			if (log.isTraceEnabled()) {
				log.trace("changeSchemeTo:[" + wsRequestUrl + "]");
			}
		}
	}

	private static class ProxyWebSocketHandler implements WebSocketHandler {

		private final WebSocketClient client;

		private final URI url;

		private final HttpHeaders headers;

		private final List<String> subProtocols;

		ProxyWebSocketHandler(URI url, WebSocketClient client, HttpHeaders headers, List<String> protocols) {
			this.client = client;
			this.url = url;
			this.headers = headers;
			if (protocols != null) {
				this.subProtocols = protocols;
			}
			else {
				this.subProtocols = Collections.emptyList();
			}
		}

		@Override
		public List<String> getSubProtocols() {
			return this.subProtocols;
		}

		@Override
		public Mono<Void> handle(WebSocketSession session) {
			// pass headers along so custom headers can be sent through
			return client.execute(url, this.headers, new WebSocketHandler() {

				private CloseStatus adaptCloseStatus(CloseStatus closeStatus) {
					int code = closeStatus.getCode();
					if (code > 2999 && code < 5000) {
						return closeStatus;
					}
					switch (code) {
						case 1000:
						case 1001:
						case 1002:
						case 1003:
						case 1007:
						case 1008:
						case 1009:
						case 1010:
						case 1011:
							return closeStatus;
						case 1004:
							// Should not be used in a close frame
							// RESERVED;
						case 1005:
							// Should not be used in a close frame
							// return CloseStatus.NO_STATUS_CODE;
						case 1006:
							// Should not be used in a close frame
							// return CloseStatus.NO_CLOSE_FRAME;
						case 1012:
							// Not in RFC6455
							// return CloseStatus.SERVICE_RESTARTED;
						case 1013:
							// Not in RFC6455
							// return CloseStatus.SERVICE_OVERLOAD;
						case 1015:
							// Should not be used in a close frame
							// return CloseStatus.TLS_HANDSHAKE_FAILURE;
						default:
							return CloseStatus.PROTOCOL_ERROR;
					}
				}

				@Override
				public Mono<Void> handle(WebSocketSession proxySession) {
					Mono<Void> serverClose = proxySession.closeStatus()
						.filter(__ -> session.isOpen())
						.map(this::adaptCloseStatus)
						.flatMap(session::close);
					Mono<Void> proxyClose = session.closeStatus()
						.filter(__ -> proxySession.isOpen())
						.map(this::adaptCloseStatus)
						.flatMap(proxySession::close);
					// Use retain() for Reactor Netty
					Mono<Void> proxySessionSend = proxySession
						.send(session.receive().doOnNext(WebSocketMessage::retain).doOnNext(webSocketMessage -> {
							if (log.isTraceEnabled()) {
								log.trace("proxySession(send from client): " + proxySession.getId()
										+ ", corresponding session:" + session.getId() + ", packet: "
										+ webSocketMessage.getPayloadAsText());
							}
						}));
					// .log("proxySessionSend", Level.FINE);
					Mono<Void> serverSessionSend = session
						.send(proxySession.receive().doOnNext(WebSocketMessage::retain).doOnNext(webSocketMessage -> {
							if (log.isTraceEnabled()) {
								log.trace("session(send from backend): " + session.getId()
										+ ", corresponding proxySession:" + proxySession.getId() + " packet: "
										+ webSocketMessage.getPayloadAsText());
							}
						}));
					// .log("sessionSend", Level.FINE);
					// Ensure closeStatus from one propagates to the other
					Mono.when(serverClose, proxyClose).subscribe();
					// Complete when both sessions are done
					return Mono.zip(proxySessionSend, serverSessionSend).then();
				}

				/**
				 * Copy subProtocols so they are available downstream.
				 * @return available subProtocols.
				 */
				@Override
				public List<String> getSubProtocols() {
					return ProxyWebSocketHandler.this.subProtocols;
				}
			});
		}

	}

}
