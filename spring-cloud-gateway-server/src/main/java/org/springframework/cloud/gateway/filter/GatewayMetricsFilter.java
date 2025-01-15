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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.support.tagsprovider.GatewayTagsProvider;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;

/**
 * 网关指标过滤器，用于记录整个请求耗时，其优先级位于{@link NettyWriteResponseFilter}之后
 *
 * @author Tony Clarke
 * @author Ingyu Hwang
 */
public class GatewayMetricsFilter implements GlobalFilter, Ordered {

	private static final Log log = LogFactory.getLog(GatewayMetricsFilter.class);

	/**
	 * 指标注册器，注册并生成对应的指标
	 */
	private final MeterRegistry meterRegistry;

	/**
	 * 网关标签提供器
	 */
	private GatewayTagsProvider compositeTagsProvider;

	/**
	 * 指标前缀
	 */
	private final String metricsPrefix;

	public GatewayMetricsFilter(MeterRegistry meterRegistry, List<GatewayTagsProvider> tagsProviders, String metricsPrefix) {
		this.meterRegistry = meterRegistry;
		this.compositeTagsProvider = tagsProviders.stream().reduce(exchange -> Tags.empty(), GatewayTagsProvider::and);
		// 去除末尾的.
		if (metricsPrefix.endsWith(".")) {
			this.metricsPrefix = metricsPrefix.substring(0, metricsPrefix.length() - 1);
		}
		else {
			this.metricsPrefix = metricsPrefix;
		}
	}

	public String getMetricsPrefix() {
		return metricsPrefix;
	}

	@Override
	public int getOrder() {
		// 尽可能早的启动计时器，并在响应返回客户端之前上报指标事件
		return NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER + 1;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
		// 启动计时器
		Sample sample = Timer.start(meterRegistry);

		return chain.filter(exchange)
				// 处理执行成功的场景
			.doOnSuccess(aVoid -> endTimerRespectingCommit(exchange, sample))
				// 处理执行异常的场景
			.doOnError(throwable -> endTimerRespectingCommit(exchange, sample));
	}

	/**
	 * 结束计时器，并根据提交状态进行处理
	 *
	 * @param exchange
	 * @param sample
	 */
	private void endTimerRespectingCommit(ServerWebExchange exchange, Sample sample) {
		// 读取服务响应
		ServerHttpResponse response = exchange.getResponse();
		if (response.isCommitted()) {
			// 对于已经提交的响应，
			endTimerInner(exchange, sample);
		}
		else {
			response.beforeCommit(() -> {
				// 在响应提交之前结束计时器
				endTimerInner(exchange, sample);
				return Mono.empty();
			});
		}
	}

	/**
	 * 结束计时器，记录耗时
	 *
	 * @param exchange
	 * @param sample
	 */
	private void endTimerInner(ServerWebExchange exchange, Sample sample) {
		// 获取标签
		Tags tags = compositeTagsProvider.apply(exchange);

		if (log.isTraceEnabled()) {
			// 打印指标前缀和标签
			log.trace(metricsPrefix + ".requests tags: " + tags);
		}
		// 暂停计时器，并记录耗时
		sample.stop(meterRegistry.timer(metricsPrefix + ".requests", tags));
	}

}
