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

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import jakarta.validation.constraints.NotEmpty;

import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

/**
 * 代表查询参数匹配的条件
 *
 * <p>任意匹配即可
 *
 * <p>配置示例:
 * <pre>
 *   ...
 *   predicates:
 * 	   - Query=color, green
 * </pre>
 *
 * @author Spencer Gibb
 */
public class QueryRoutePredicateFactory extends AbstractRoutePredicateFactory<QueryRoutePredicateFactory.Config> {

	/**
	 * Param key.
	 */
	public static final String PARAM_KEY = "param";

	/**
	 * Regexp key.
	 */
	public static final String REGEXP_KEY = "regexp";

	public QueryRoutePredicateFactory() {
		super(Config.class);
	}

	@Override
	public List<String> shortcutFieldOrder() {
		return Arrays.asList(PARAM_KEY, REGEXP_KEY);
	}

	@Override
	public Predicate<ServerWebExchange> apply(Config config) {
		return new GatewayPredicate() {
			@Override
			public boolean test(ServerWebExchange exchange) {
				if (!StringUtils.hasText(config.regexp)) {
					// check existence of header
					return exchange.getRequest().getQueryParams().containsKey(config.param);
				}

				// 类似示例中的查询参数color对应的值
				List<String> values = exchange.getRequest().getQueryParams().get(config.param);
				if (values == null) {
					return false;
				}
				for (String value : values) {
					// 检查参数值是否匹配类似上述的green
					if (value != null && value.matches(config.regexp)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public Object getConfig() {
				return config;
			}

			@Override
			public String toString() {
				return String.format("Query: param=%s regexp=%s", config.getParam(), config.getRegexp());
			}
		};
	}

	/**
	 * 代表Query的配置值
	 */
	public static class Config {

		/**
		 * 类似上述示例中的color
		 */
		@NotEmpty
		private String param;

		/**
		 * 类似上述示例中的green
		 */
		private String regexp;

		public String getParam() {
			return param;
		}

		public Config setParam(String param) {
			this.param = param;
			return this;
		}

		public String getRegexp() {
			return regexp;
		}

		public Config setRegexp(String regexp) {
			this.regexp = regexp;
			return this;
		}

	}

}
