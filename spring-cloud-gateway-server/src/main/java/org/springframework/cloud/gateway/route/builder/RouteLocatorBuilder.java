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

package org.springframework.cloud.gateway.route.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import reactor.core.publisher.Flux;

import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 用于构造{@link RouteLocator}的构造器
 *
 * <p>Builder设计模式
 */
public class RouteLocatorBuilder {

	private ConfigurableApplicationContext context;

	public RouteLocatorBuilder(ConfigurableApplicationContext context) {
		this.context = context;
	}

	/**
	 * Creates a new {@link Builder}.
	 * @return a new {@link Builder}.
	 */
	public Builder routes() {
		return new Builder(context);
	}

	/**
	 * A class that can be used to construct routes and return a {@link RouteLocator}.
	 */
	public static class Builder {

		private List<Buildable<Route>> routes = new ArrayList<>();

		private ConfigurableApplicationContext context;

		public Builder(ConfigurableApplicationContext context) {
			this.context = context;
		}

		/**
		 * 使用id和条件构造一个{@link Route}
		 *
		 * @param id 用于路由的唯一Id
		 * @param fn a function which takes in a {@link PredicateSpec} and returns a
		 * {@link Route.AsyncBuilder}
		 * @return a {@link Builder}
		 */
		public Builder route(String id, Function<PredicateSpec, Buildable<Route>> fn) {
			Buildable<Route> routeBuilder = fn.apply(new RouteSpec(this).id(id));
			add(routeBuilder);
			return this;
		}

		/**
		 * 使用条件构造一个{@link Route}，其唯一id使用{@link RouteSpec#randomId()}生成随机值
		 *
		 * @param fn a function which takes in a {@link PredicateSpec} and returns a
		 * {@link Route.AsyncBuilder}
		 * @return a {@link Builder}
		 */
		public Builder route(Function<PredicateSpec, Buildable<Route>> fn) {
			Buildable<Route> routeBuilder = fn.apply(new RouteSpec(this).randomId());
			add(routeBuilder);
			return this;
		}

		/**
		 * 构造并返回{@link RouteLocator}
		 *
		 * @return a {@link RouteLocator}
		 */
		public RouteLocator build() {
			return () -> Flux.fromIterable(this.routes).map(routeBuilder -> routeBuilder.build());
		}

		ConfigurableApplicationContext getContext() {
			return context;
		}

		void add(Buildable<Route> route) {
			routes.add(route);
		}

	}

	public static class RouteSpec {

		private final Route.AsyncBuilder routeBuilder = Route.async();

		private final Builder builder;

		RouteSpec(Builder builder) {
			this.builder = builder;
		}

		public PredicateSpec id(String id) {
			this.routeBuilder.id(id);
			return predicateBuilder();
		}

		public PredicateSpec randomId() {
			return id(UUID.randomUUID().toString());
		}

		private PredicateSpec predicateBuilder() {
			return new PredicateSpec(this.routeBuilder, this.builder);
		}

	}

}
