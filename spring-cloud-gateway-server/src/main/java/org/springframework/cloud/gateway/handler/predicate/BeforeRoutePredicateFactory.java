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

package org.springframework.cloud.gateway.handler.predicate;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import jakarta.validation.constraints.NotNull;

import org.springframework.web.server.ServerWebExchange;

/**
 * 代表在指定时间之前的条件
 *
 * <p>配置示例:
 * <pre>
 * 	spring:
 * 	  cloud:
 * 	    gateway:
 * 	      routes:
 * 	        -id: before_route
 * 	        uri: https://example.org
 * 	        predicates:
 * 	          - Before=2017-01-20T17:42:47.789-07:00[America/Denver]
 * </pre>
 *
 * @author Spencer Gibb
 */
public class BeforeRoutePredicateFactory extends AbstractRoutePredicateFactory<BeforeRoutePredicateFactory.Config> {

	/**
	 * DateTime key.
	 */
	public static final String DATETIME_KEY = "datetime";

	public BeforeRoutePredicateFactory() {
		super(Config.class);
	}

	@Override
	public List<String> shortcutFieldOrder() {
		return Collections.singletonList(DATETIME_KEY);
	}

	@Override
	public Predicate<ServerWebExchange> apply(Config config) {
		return new GatewayPredicate() {
			@Override
			public boolean test(ServerWebExchange serverWebExchange) {
				final ZonedDateTime now = ZonedDateTime.now();
				// 对比当前时间是否早于配置的时间
				return now.isBefore(config.getDatetime());
			}

			@Override
			public Object getConfig() {
				return config;
			}

			@Override
			public String toString() {
				return String.format("Before: %s", config.getDatetime());
			}
		};
	}

	/**
	 * 代表Before的配置值
	 */
	public static class Config {

		/**
		 * 类似上述示例中的2017-01-20T17:42:47.789-07:00[America/Denver]
		 */
		@NotNull
		private ZonedDateTime datetime;

		public ZonedDateTime getDatetime() {
			return datetime;
		}

		public void setDatetime(ZonedDateTime datetime) {
			this.datetime = datetime;
		}

	}

}
