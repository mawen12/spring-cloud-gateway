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

package org.springframework.cloud.gateway.route;

import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.HeartbeatMonitor;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.cloud.client.discovery.event.ParentHeartbeatEvent;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;

/**
 * 路由刷新监听器，为动态路由提供了能力
 */
// see ZuulDiscoveryRefreshListener
// TODO: make abstract class in commons?
public class RouteRefreshListener implements ApplicationListener<ApplicationEvent> {

	/**
	 * 时间发布器
	 */
	private final ApplicationEventPublisher publisher;

	/**
	 * 心跳监控
	 */
	private HeartbeatMonitor monitor = new HeartbeatMonitor();

	public RouteRefreshListener(ApplicationEventPublisher publisher) {
		Assert.notNull(publisher, "publisher may not be null");
		this.publisher = publisher;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		if (event instanceof ContextRefreshedEvent) {
			// 处理上下文刷新的事件
			ContextRefreshedEvent refreshedEvent = (ContextRefreshedEvent) event;
			// 检查是否为management上下文
			boolean isManagementCtxt = WebServerApplicationContext.hasServerNamespace(refreshedEvent.getApplicationContext(), "management");
			// 检查是否为LoadBalancerClientFactory-上下文
			boolean isLoadBalancerCtxt = refreshedEvent.getApplicationContext().getDisplayName() != null && refreshedEvent.getApplicationContext().getDisplayName().startsWith("LoadBalancerClientFactory-");

			// 不处理management和LoadBalancerClientFactory-的上下文
			if (!isManagementCtxt && !isLoadBalancerCtxt) {
				reset();
			}
		}
		else if (event instanceof RefreshScopeRefreshedEvent || event instanceof InstanceRegisteredEvent) {
			reset();
		}
		else if (event instanceof ParentHeartbeatEvent) {
			ParentHeartbeatEvent e = (ParentHeartbeatEvent) event;
			resetIfNeeded(e.getValue());
		}
		else if (event instanceof HeartbeatEvent) {
			HeartbeatEvent e = (HeartbeatEvent) event;
			resetIfNeeded(e.getValue());
		}
	}

	private void resetIfNeeded(Object value) {
		if (this.monitor.update(value)) {
			reset();
		}
	}

	private void reset() {
		// 发送刷新路由事件
		this.publisher.publishEvent(new RefreshRoutesEvent(this));
	}

}
