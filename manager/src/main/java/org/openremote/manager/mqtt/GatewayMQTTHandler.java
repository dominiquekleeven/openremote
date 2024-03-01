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
import org.openremote.manager.asset.AssetProcessingService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.manager.security.ManagerKeycloakIdentityProvider;
import org.openremote.model.Container;
import org.openremote.model.asset.GatewayClientType;
import org.openremote.model.asset.agent.ConnectionStatus;
import org.openremote.model.asset.impl.GatewayAsset;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.query.AssetQuery;
import org.openremote.model.syslog.SyslogCategory;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.logging.Logger;

import static org.openremote.manager.mqtt.Topic.SINGLE_LEVEL_TOKEN;
import static org.openremote.model.syslog.SyslogCategory.API;

public class GatewayMQTTHandler extends MQTTHandler {

    public static final String GATEWAY_TOPIC = "gateway";
    public static final String ASSETS_TOPIC = "assets";
    public static final String ATTRIBUTES_TOPIC = "attributes";
    protected static final Logger LOG = SyslogCategory.getLogger(API, GatewayMQTTHandler.class);
    protected AssetProcessingService assetProcessingService;
    protected TimerService timerService;
    protected AssetStorageService assetStorageService;
    protected ManagerKeycloakIdentityProvider identityProvider;
    protected boolean isKeycloak;

    @Override
    public void start(Container container) throws Exception {
        super.start(container);
        LOG.info("Starting GatewayMQTTHandler");
        timerService = container.getService(TimerService.class);
        assetStorageService = container.getService(AssetStorageService.class);
        assetProcessingService = container.getService(AssetProcessingService.class);
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
        updateGatewayAssetStatusIfLinked(connection, ConnectionStatus.CONNECTED);
    }

    @Override
    public boolean handlesTopic(Topic topic) {
        return topicMatches(topic);
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
                SINGLE_LEVEL_TOKEN + "/" + SINGLE_LEVEL_TOKEN + "/" + GATEWAY_TOPIC + ASSETS_TOPIC + "/" + SINGLE_LEVEL_TOKEN + "/"
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
        updateGatewayAssetStatusIfLinked(connection, ConnectionStatus.DISCONNECTED);
    }

    @Override
    public void onDisconnect(RemotingConnection connection) {
        updateGatewayAssetStatusIfLinked(connection, ConnectionStatus.DISCONNECTED);
    }

    protected void sendAttributeEvent(AttributeEvent event) {
        assetProcessingService.sendAttributeEvent(event, GatewayMQTTHandler.class.getName());
    }

    protected void updateGatewayAssetStatusIfLinked(RemotingConnection connection, ConnectionStatus status) {
        GatewayAsset gatewayAsset = (GatewayAsset) assetStorageService.find(new AssetQuery()
                .types(GatewayAsset.class).attributeValue(GatewayAsset.CLIENT_ID.getName(), connection.getClientID()));

        if (gatewayAsset != null && gatewayAsset.getGatewayClientType().isPresent()) {
            if (gatewayAsset.getGatewayClientType().get() == GatewayClientType.MQTT) {
                LOG.fine("Linked Gateway asset found for MQTT client, updating status to " + status);
                sendAttributeEvent(new AttributeEvent(gatewayAsset.getId(), GatewayAsset.STATUS, status));
            }
        }
    }

}
