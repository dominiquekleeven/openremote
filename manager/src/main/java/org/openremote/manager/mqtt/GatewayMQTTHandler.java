/*
 * Copyright 2021, OpenRemote Inc.
 *
 * See the CONTRIBUTORS.txt file in the distribution for a
 * full listing of individual contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.openremote.manager.mqtt;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.keycloak.KeycloakSecurityContext;
import org.openremote.container.timer.TimerService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.manager.security.ManagerKeycloakIdentityProvider;
import org.openremote.model.Container;
import org.openremote.model.syslog.SyslogCategory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static org.openremote.manager.mqtt.Topic.SINGLE_LEVEL_TOKEN;
import static org.openremote.model.syslog.SyslogCategory.API;

public class GatewayMQTTHandler extends MQTTHandler {

    protected static final Logger LOG = SyslogCategory.getLogger(API, GatewayMQTTHandler.class);
    public static final String GATEWAY_TOPIC = "gateway";
    public static final String ASSETS_TOPIC = "assets";
    public static final String ATTRIBUTES_TOPIC = "attributes";

    protected TimerService timerService;
    protected AssetStorageService assetStorageService;
    protected ManagerKeycloakIdentityProvider identityProvider;
    protected boolean isKeycloak;
    protected final ConcurrentMap<Long, Set<RemotingConnection>> provisioningConfigAuthenticatedConnectionMap = new ConcurrentHashMap<>();

    @Override
    public void start(Container container) throws Exception {
        super.start(container);
        LOG.info("Starting GatewayMQTTHandler");
        timerService = container.getService(TimerService.class);
        assetStorageService = container.getService(AssetStorageService.class);
        ManagerIdentityService identityService = container.getService(ManagerIdentityService.class);

        if (!identityService.isKeycloakEnabled()) {
            LOG.warning("MQTT connections are not supported when not using Keycloak identity provider");
            isKeycloak = false;
        } else {
            isKeycloak = true;
            identityProvider = (ManagerKeycloakIdentityProvider) identityService.getIdentityProvider();
        }
    }

    @Override
    public void onConnect(RemotingConnection connection) {
        super.onConnect(connection);
        LOG.fine("New MQTT connection session " + MQTTBrokerService.connectionToString(connection));
    }

    @Override
    public boolean handlesTopic(Topic topic) {
        return topicMatches(topic);
    }

    @Override
    public boolean checkCanSubscribe(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        if (!canSubscribe(connection, securityContext, topic)) {
            getLogger().fine("Cannot subscribe to this topic, topic=" + topic + ", " + MQTTBrokerService.connectionToString(connection));
            return false;
        }
        return true;
    }

    @Override
    public boolean checkCanPublish(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        if (!canPublish(connection, securityContext, topic)) {
            getLogger().fine("Cannot publish to this topic, topic=" + topic + ", " + MQTTBrokerService.connectionToString(connection));
            return false;
        }
        return true;
    }

    @Override
    public boolean topicMatches(Topic topic) {
        return GATEWAY_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, 2));
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public boolean canSubscribe(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        if (!isKeycloak) {
            LOG.fine("Identity provider is not keycloak");
            return false;
        }

        return true;
    }

    @Override
    public void onSubscribe(RemotingConnection connection, Topic topic) {
    }

    @Override
    public void onUnsubscribe(RemotingConnection connection, Topic topic) {

    }

    @Override
    public Set<String> getPublishListenerTopics() {
        return Set.of(
            SINGLE_LEVEL_TOKEN + "/" + SINGLE_LEVEL_TOKEN + "/" + GATEWAY_TOPIC  + ASSETS_TOPIC + "/" + SINGLE_LEVEL_TOKEN + "/"
        );
    }

    @Override
    public boolean canPublish(RemotingConnection connection, KeycloakSecurityContext securityContext, Topic topic) {
        if (!isKeycloak) {
            LOG.fine("Identity provider is not keycloak");
            return false;
        }

        return true;
    }

    @Override
    public void onPublish(RemotingConnection connection, Topic topic, ByteBuf body) {
        String payloadContent = body.toString(StandardCharsets.UTF_8);
        LOG.fine("Received message on topic " + topic + " with payload " + payloadContent);
    }

    @Override
    public void onConnectionLost(RemotingConnection connection) {
        provisioningConfigAuthenticatedConnectionMap.values().forEach(connections -> connections.remove(connection));
    }

    @Override
    public void onDisconnect(RemotingConnection connection) {
        provisioningConfigAuthenticatedConnectionMap.values().forEach(connections -> connections.remove(connection));
    }





}
