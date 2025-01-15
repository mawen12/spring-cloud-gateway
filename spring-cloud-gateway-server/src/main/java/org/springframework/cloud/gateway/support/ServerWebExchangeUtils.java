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

package org.springframework.cloud.gateway.support;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.cloud.gateway.filter.factory.GatewayFilterFactory;
import org.springframework.cloud.gateway.handler.AsyncPredicate;
import org.springframework.cloud.gateway.handler.predicate.RoutePredicateFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.PooledDataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.AbstractServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.util.Assert;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * @author Spencer Gibb
 */
public final class ServerWebExchangeUtils {

	private static final Log log = LogFactory.getLog(ServerWebExchangeUtils.class);

	/**
	 * 保留主机头属性，如果选择保留，则在进行新的http请求之前，将主机写入到新请求头中
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 */
	public static final String PRESERVE_HOST_HEADER_ATTRIBUTE = qualify("preserveHostHeader");

	/**
	 * URI template variables attribute name.
	 */
	public static final String URI_TEMPLATE_VARIABLES_ATTRIBUTE = qualify("uriTemplateVariables");

	/**
	 * 客户端响应属性，在执行到路由过滤器并发起请求后，将保存客户端的响应
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 */
	public static final String CLIENT_RESPONSE_ATTR = qualify("gatewayClientResponse");

	/**
	 * 客户端响应连接属性，在执行到路由过滤器并发起请求后，保存返回客户端响应连接
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 */
	public static final String CLIENT_RESPONSE_CONN_ATTR = qualify("gatewayClientResponseConnection");

	/**
	 * 客户端响应头名称集合属性，在执行到路由过滤器并发起请求后，保存返回响应头的名称集合
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 */
	public static final String CLIENT_RESPONSE_HEADER_NAMES = qualify("gatewayClientResponseHeaderNames");

	/**
	 * Gateway route attribute name.
	 */
	public static final String GATEWAY_ROUTE_ATTR = qualify("gatewayRoute");

	/**
	 * Original Reactor Context corresponding to the processed request.
	 */
	public static final String GATEWAY_REACTOR_CONTEXT_ATTR = qualify("gatewayReactorContext");

	/**
	 * Gateway request URL attribute name.
	 */
	public static final String GATEWAY_REQUEST_URL_ATTR = qualify("gatewayRequestUrl");

	/**
	 * Gateway original request URL attribute name.
	 */
	public static final String GATEWAY_ORIGINAL_REQUEST_URL_ATTR = qualify("gatewayOriginalRequestUrl");

	/**
	 * Gateway handler mapper attribute name.
	 */
	public static final String GATEWAY_HANDLER_MAPPER_ATTR = qualify("gatewayHandlerMapper");

	/**
	 * Gateway scheme prefix attribute name.
	 */
	public static final String GATEWAY_SCHEME_PREFIX_ATTR = qualify("gatewaySchemePrefix");

	/**
	 * Gateway predicate route attribute name.
	 */
	public static final String GATEWAY_PREDICATE_ROUTE_ATTR = qualify("gatewayPredicateRouteAttr");

	/**
	 * Gateway predicate matched path attribute name.
	 */
	public static final String GATEWAY_PREDICATE_MATCHED_PATH_ATTR = qualify("gatewayPredicateMatchedPathAttr");

	/**
	 * Gateway predicate matched path route id attribute name.
	 */
	public static final String GATEWAY_PREDICATE_MATCHED_PATH_ROUTE_ID_ATTR = qualify(
			"gatewayPredicateMatchedPathRouteIdAttr");

	/**
	 * Gateway predicate path container attribute name.
	 */
	public static final String GATEWAY_PREDICATE_PATH_CONTAINER_ATTR = qualify("gatewayPredicatePathContainer");

	/**
	 * Weight attribute name.
	 */
	public static final String WEIGHT_ATTR = qualify("routeWeight");

	/**
	 * 原始响应的内容类型属性，在执行到路由过滤器并发起请求后，如果响应头的内容类型不为空，则写入
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 */
	public static final String ORIGINAL_RESPONSE_CONTENT_TYPE_ATTR = "original_response_content_type";

	/**
	 * CircuitBreaker execution exception attribute name.
	 */
	public static final String CIRCUITBREAKER_EXECUTION_EXCEPTION_ATTR = qualify("circuitBreakerExecutionException");

	/**
	 * 当一个路由过滤器已经被成功调用的时候使用。允许开发者重写自定义的路由过滤器来禁用内置的路由过滤器
	 *
	 * @see org.springframework.cloud.gateway.filter.NettyRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.ForwardRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebClientHttpRoutingFilter
	 * @see org.springframework.cloud.gateway.filter.WebsocketRoutingFilter
	 */
	public static final String GATEWAY_ALREADY_ROUTED_ATTR = qualify("gatewayAlreadyRouted");

	/**
	 * Gateway already prefixed attribute name.
	 */
	public static final String GATEWAY_ALREADY_PREFIXED_ATTR = qualify("gatewayAlreadyPrefixed");

	/**
	 * Cached ServerHttpRequestDecorator attribute name. Used when
	 * {@link #cacheRequestBodyAndRequest(ServerWebExchange, Function)} is called.
	 */
	public static final String CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR = "cachedServerHttpRequestDecorator";

	/**
	 * Cached request body key. Used when
	 * {@link #cacheRequestBodyAndRequest(ServerWebExchange, Function)} or
	 * {@link #cacheRequestBody(ServerWebExchange, Function)} are called.
	 */
	public static final String CACHED_REQUEST_BODY_ATTR = "cachedRequestBody";

	/**
	 * Gateway LoadBalancer {@link Response} attribute name.
	 */
	public static final String GATEWAY_LOADBALANCER_RESPONSE_ATTR = qualify("gatewayLoadBalancerResponse");

	/**
	 * Gateway Client {@code Observation} attribute name.
	 */
	public static final String GATEWAY_OBSERVATION_ATTR = qualify("gateway.observation");

	private static final byte[] EMPTY_BYTES = {};

	private ServerWebExchangeUtils() {
		throw new AssertionError("Must not instantiate utility class.");
	}

	private static String qualify(String attr) {
		return ServerWebExchangeUtils.class.getName() + "." + attr;
	}

	public static void setAlreadyRouted(ServerWebExchange exchange) {
		exchange.getAttributes().put(GATEWAY_ALREADY_ROUTED_ATTR, true);
	}

	public static void removeAlreadyRouted(ServerWebExchange exchange) {
		exchange.getAttributes().remove(GATEWAY_ALREADY_ROUTED_ATTR);
	}

	public static boolean isAlreadyRouted(ServerWebExchange exchange) {
		return exchange.getAttributeOrDefault(GATEWAY_ALREADY_ROUTED_ATTR, false);
	}

	public static boolean setResponseStatus(ServerWebExchange exchange, HttpStatus httpStatus) {
		boolean response = exchange.getResponse().setStatusCode(httpStatus);
		if (!response && log.isWarnEnabled()) {
			log.warn("Unable to set status code to " + httpStatus + ". Response already committed.");
		}
		return response;
	}

	public static void reset(ServerWebExchange exchange) {
		// TODO: what else to do to reset exchange?
		Set<String> addedHeaders = exchange.getAttributeOrDefault(CLIENT_RESPONSE_HEADER_NAMES, Collections.emptySet());
		addedHeaders.forEach(header -> exchange.getResponse().getHeaders().remove(header));
		removeAlreadyRouted(exchange);
	}

	public static boolean setResponseStatus(ServerWebExchange exchange, HttpStatusHolder statusHolder) {
		if (exchange.getResponse().isCommitted()) {
			return false;
		}
		if (log.isDebugEnabled()) {
			log.debug("Setting response status to " + statusHolder);
		}
		if (statusHolder.getHttpStatus() != null) {
			return setResponseStatus(exchange, statusHolder.getHttpStatus());
		}
		if (statusHolder.getStatus() != null && exchange.getResponse() instanceof AbstractServerHttpResponse) { // non-standard
			((AbstractServerHttpResponse) exchange.getResponse()).setRawStatusCode(statusHolder.getStatus());
			return true;
		}
		return false;
	}

	public static boolean containsEncodedParts(URI uri) {
		boolean encoded = (uri.getRawQuery() != null && uri.getRawQuery().contains("%"))
				|| (uri.getRawPath() != null && uri.getRawPath().contains("%"));

		// Verify if it is really fully encoded. Treat partial encoded as unencoded.
		if (encoded) {
			try {
				UriComponentsBuilder.fromUri(uri).build(true);
				return true;
			} catch (IllegalArgumentException ignored) {
				if (log.isTraceEnabled()) {
					log.trace("Error in containsEncodedParts", ignored);
				}
			}

			return false;
		}

		return encoded;
	}

	public static HttpStatus parse(String statusString) {
		HttpStatus httpStatus;

		try {
			int status = Integer.parseInt(statusString);
			httpStatus = HttpStatus.resolve(status);
		} catch (NumberFormatException e) {
			// try the enum string
			httpStatus = HttpStatus.valueOf(statusString.toUpperCase(Locale.ROOT));
		}
		return httpStatus;
	}

	public static void addOriginalRequestUrl(ServerWebExchange exchange, URI url) {
		// 初始化请求头 gatewayOriginalRequestUrl
		exchange.getAttributes().computeIfAbsent(GATEWAY_ORIGINAL_REQUEST_URL_ATTR, s -> new LinkedHashSet<>());
		// 获取请求头 gatewayOriginalRequestUrl
		LinkedHashSet<URI> uris = exchange.getRequiredAttribute(GATEWAY_ORIGINAL_REQUEST_URL_ATTR);
		// 保存原始路径
		uris.add(url);
	}

	public static AsyncPredicate<ServerWebExchange> toAsyncPredicate(Predicate<? super ServerWebExchange> predicate) {
		Assert.notNull(predicate, "predicate must not be null");
		return AsyncPredicate.from(predicate);
	}

	public static String expand(ServerWebExchange exchange, String template) {
		Assert.notNull(exchange, "exchange may not be null");
		Assert.notNull(template, "template may not be null");

		if (template.indexOf('{') == -1) { // short circuit
			return template;
		}

		Map<String, String> variables = getUriTemplateVariables(exchange);
		return UriComponentsBuilder.fromPath(template).build().expand(variables).getPath();
	}

	@SuppressWarnings("unchecked")
	public static void putUriTemplateVariables(ServerWebExchange exchange, Map<String, String> uriVariables) {
		if (exchange.getAttributes().containsKey(URI_TEMPLATE_VARIABLES_ATTRIBUTE)) {
			Map<String, Object> existingVariables = (Map<String, Object>) exchange.getAttributes()
					.get(URI_TEMPLATE_VARIABLES_ATTRIBUTE);
			HashMap<String, Object> newVariables = new HashMap<>();
			newVariables.putAll(existingVariables);
			newVariables.putAll(uriVariables);
			exchange.getAttributes().put(URI_TEMPLATE_VARIABLES_ATTRIBUTE, newVariables);
		} else {
			exchange.getAttributes().put(URI_TEMPLATE_VARIABLES_ATTRIBUTE, uriVariables);
		}
	}

	public static Map<String, String> getUriTemplateVariables(ServerWebExchange exchange) {
		return exchange.getAttributeOrDefault(URI_TEMPLATE_VARIABLES_ATTRIBUTE, new HashMap<>());
	}

	/**
	 * Caches the request body and the created {@link ServerHttpRequestDecorator} in
	 * ServerWebExchange attributes. Those attributes are
	 * {@link #CACHED_REQUEST_BODY_ATTR} and
	 * {@link #CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR} respectively. This method is
	 * useful when the {@link ServerWebExchange} can not be modified, such as a
	 * {@link RoutePredicateFactory}.
	 *
	 * @param exchange the available ServerWebExchange.
	 * @param function a function that accepts the created ServerHttpRequestDecorator.
	 * @param <T>      generic type for the return {@link Mono}.
	 *
	 * @return Mono of type T created by the function parameter.
	 */
	public static <T> Mono<T> cacheRequestBodyAndRequest(ServerWebExchange exchange,
														 Function<ServerHttpRequest, Mono<T>> function) {
		return cacheRequestBody(exchange, true, function);
	}

	/**
	 * Caches the request body in a ServerWebExchange attributes. The attribute is
	 * {@link #CACHED_REQUEST_BODY_ATTR}. This method is useful when the
	 * {@link ServerWebExchange} can be mutated, such as a {@link GatewayFilterFactory}.
	 *
	 * @param exchange the available ServerWebExchange.
	 * @param function a function that accepts the created ServerHttpRequestDecorator.
	 * @param <T>      generic type for the return {@link Mono}.
	 *
	 * @return Mono of type T created by the function parameter.
	 */
	public static <T> Mono<T> cacheRequestBody(ServerWebExchange exchange,
											   Function<ServerHttpRequest, Mono<T>> function) {
		return cacheRequestBody(exchange, false, function);
	}

	/**
	 * Caches the request body in a ServerWebExchange attribute. The attribute is
	 * {@link #CACHED_REQUEST_BODY_ATTR}. If this method is called from a location that
	 * can not mutate the ServerWebExchange (such as a Predicate), setting
	 * cacheDecoratedRequest to true will put a {@link ServerHttpRequestDecorator} in an
	 * attribute {@link #CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR} for adaptation later.
	 *
	 * @param exchange              the available ServerWebExchange.
	 * @param cacheDecoratedRequest if true, the ServerHttpRequestDecorator will be
	 *                              cached.
	 * @param function              a function that accepts a ServerHttpRequest. It can be the created
	 *                              ServerHttpRequestDecorator or the original if there is no body.
	 * @param <T>                   generic type for the return {@link Mono}.
	 *
	 * @return Mono of type T created by the function parameter.
	 */
	private static <T> Mono<T> cacheRequestBody(ServerWebExchange exchange, boolean cacheDecoratedRequest,
												Function<ServerHttpRequest, Mono<T>> function) {
		// don't cache if body is already cached
		Object cachedDataBuffer = exchange.getAttribute(CACHED_REQUEST_BODY_ATTR);
		if (cachedDataBuffer instanceof DataBuffer) {
			if (log.isTraceEnabled()) {
				log.trace("body already in exchange attribute, short circuiting");
			}
			return Mono.just(exchange.getRequest()).flatMap(function);
		}
		ServerHttpResponse response = exchange.getResponse();
		DataBufferFactory factory = response.bufferFactory();
		// Join all the DataBuffers so we have a single DataBuffer for the body
		return DataBufferUtils.join(exchange.getRequest().getBody())
				.defaultIfEmpty(factory.wrap(EMPTY_BYTES))
				.map(dataBuffer -> decorate(exchange, dataBuffer, cacheDecoratedRequest))
				.switchIfEmpty(Mono.just(exchange.getRequest()))
				.flatMap(function);
	}

	/**
	 * 清除{@link ServerWebExchange}中的请求体，该请求体保存在{@link org.springframework.web.server.adapter.DefaultServerWebExchange#attributes}中，
	 * 属性名称为{@link #CACHED_REQUEST_BODY_ATTR}
	 *
	 * <p>如果使用的是{@link PooledDataBuffer}，对于已经分配的空间需要手动释放
	 *
	 * @param exchange the available ServerWebExchange.
	 */
	public static void clearCachedRequestBody(ServerWebExchange exchange) {
		// 移除属性 cachedRequestBody
		Object attribute = exchange.getAttributes().remove(CACHED_REQUEST_BODY_ATTR);
		if (attribute != null && attribute instanceof PooledDataBuffer) {
			// 对于使用 PooledDataBuffer 来缓冲数据，如果已经分配了空间，则需要回收
			PooledDataBuffer dataBuffer = (PooledDataBuffer) attribute;
			if (dataBuffer.isAllocated()) {
				if (log.isTraceEnabled()) {
					log.trace("releasing cached body in exchange attribute");
				}
				// 确保清除缓冲数据
				while (!dataBuffer.release()) {
					// 一直调用release()，直到其计数器变为0，该方法不会永远循环
				}
			}
		}
	}

	private static ServerHttpRequest decorate(ServerWebExchange exchange, DataBuffer dataBuffer,
											  boolean cacheDecoratedRequest) {
		if (dataBuffer.readableByteCount() > 0) {
			if (log.isTraceEnabled()) {
				log.trace("retaining body in exchange attribute");
			}

			Object cachedDataBuffer = exchange.getAttribute(CACHED_REQUEST_BODY_ATTR);
			// don't cache if body is already cached
			if (!(cachedDataBuffer instanceof DataBuffer)) {
				exchange.getAttributes().put(CACHED_REQUEST_BODY_ATTR, dataBuffer);
			}
		}

		ServerHttpRequest decorator = new ServerHttpRequestDecorator(exchange.getRequest()) {
			@Override
			public Flux<DataBuffer> getBody() {
				return Mono.fromSupplier(() -> {
					if (exchange.getAttribute(CACHED_REQUEST_BODY_ATTR) == null) {
						// probably == downstream closed or no body
						return null;
					}
					if (dataBuffer instanceof NettyDataBuffer) {
						NettyDataBuffer pdb = (NettyDataBuffer) dataBuffer;
						return pdb.factory().wrap(pdb.getNativeBuffer().retainedSlice());
					} else if (dataBuffer instanceof DefaultDataBuffer) {
						DefaultDataBuffer ddf = (DefaultDataBuffer) dataBuffer;
						return ddf.factory().wrap(Unpooled.wrappedBuffer(ddf.getNativeBuffer()).nioBuffer());
					} else {
						throw new IllegalArgumentException(
								"Unable to handle DataBuffer of type " + dataBuffer.getClass());
					}
				}).flux();
			}
		};
		if (cacheDecoratedRequest) {
			exchange.getAttributes().put(CACHED_SERVER_HTTP_REQUEST_DECORATOR_ATTR, decorator);
		}
		return decorator;
	}

	/**
	 * One place to handle forwarding using DispatcherHandler. Allows for common code to
	 * be reused.
	 *
	 * @param handler  The DispatcherHandler.
	 * @param exchange The ServerWebExchange.
	 *
	 * @return value from handler.
	 */
	public static Mono<Void> handle(DispatcherHandler handler, ServerWebExchange exchange) {
		// remove attributes that may disrupt the forwarded request
		exchange.getAttributes().remove(GATEWAY_PREDICATE_PATH_CONTAINER_ATTR);

		// CORS check is applied to the original request, but should not be applied to
		// internally forwarded requests.
		// See https://github.com/spring-cloud/spring-cloud-gateway/issues/3350.
		exchange = exchange.mutate().request(request -> request.headers(headers -> headers.setOrigin(null))).build();

		return handler.handle(exchange);
	}

}
