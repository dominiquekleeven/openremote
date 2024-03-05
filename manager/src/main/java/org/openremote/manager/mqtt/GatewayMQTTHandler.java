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
import org.openremote.model.asset.agent.ConnectionStatus;
import org.openremote.model.asset.impl.GatewayAsset;
import org.openremote.model.asset.impl.GatewayV2Asset;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.query.AssetQuery;
import org.openremote.model.syslog.SyslogCategory;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.logging.Logger;

import static org.openremote.manager.mqtt.Topic.SINGLE_LEVEL_TOKEN;
import static org.openremote.model.syslog.SyslogCategory.API;

public class GatewayMQTTHandler extends MQTTHandler {

    // main topics
    public static final String GATEWAY_TOPIC = "gateway"; // handler prefix
    public static final String ASSETS_TOPIC = "assets";
    public static final String ATTRIBUTES_TOPIC = "attributes";
    public static final String PROVISION_TOPIC = "provision";
    public static final String HEALTH_TOPIC = "health";

    // method topics
    public static final String CREATE_TOPIC = "create";
    public static final String READ_TOPIC = "read";
    public static final String UPDATE_TOPIC = "update";
    public static final String DELETE_TOPIC = "delete";

    // response topics
    public static final String RESPONSE_TOPIC = "response";

    // consistent token indexes for the topics
    public static final int ATTRIBUTES_TOKEN_INDEX = 5;
    public static final int ASSETS_TOKEN_INDEX = 3;
    public static final int HEALTH_TOKEN_INDEX = 3;
    public static final int GATEWAY_PREFIX_TOKEN_INDEX = 2;
    public static final int GATEWAY_PREFIX_PROVISION_TOKEN_INDEX = 0;

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
        return GATEWAY_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, GATEWAY_PREFIX_TOKEN_INDEX));
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


    // Publish topics that the handler listens to - these are the topics that the handler is subscribed to
    @Override
    public Set<String> getPublishListenerTopics() {
        return Set.of(

                // single-line attribute update
                // Path: <REALM> / <CLIENT_ID> / gateway / assets / <ASSET_ID> / attributes / <ATTRIBUTE_NAME> / update
                // token 3 = assets, token 4 = assetId, token 5 = attributes, token 6 = attributeName, token 7 = update
                SINGLE_LEVEL_TOKEN + "/" + SINGLE_LEVEL_TOKEN + "/" + GATEWAY_TOPIC + "/" + ASSETS_TOPIC + "/" + SINGLE_LEVEL_TOKEN + "/" + ATTRIBUTES_TOPIC + "/" + SINGLE_LEVEL_TOKEN + "/" + UPDATE_TOPIC,

                // multi-line attribute update
                // PATH: <REALM> / <CLIENT_ID> / gateway / assets / <ASSET_ID> / attributes / update
                // token 3 = assets, token 4 = assetId, token 5 = attributes, token 6 = update
                SINGLE_LEVEL_TOKEN + "/" + SINGLE_LEVEL_TOKEN + "/" + GATEWAY_TOPIC + "/" + ASSETS_TOPIC + "/" + SINGLE_LEVEL_TOKEN + "/" + ATTRIBUTES_TOPIC + "/" + UPDATE_TOPIC,

                // test
                // Path: <REALM> / <CLIENT_ID> / gateway / health
                // token 3 = test
                SINGLE_LEVEL_TOKEN + "/" + SINGLE_LEVEL_TOKEN + "/" + GATEWAY_TOPIC + "/" + "health"
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


    protected void handleAttributesUpdateRequest(RemotingConnection connection, Topic topic, ByteBuf body) {

        if (!isAttributesTopic(topic)) {
            LOG.warning("Trying to update attributes on non-attributes topic " + topic);
            return;
        }

        String assetId = topicTokenIndexToString(topic, 4);
        if (assetId == null) {
            LOG.warning("Received attributes update message with no associated asset ID");
            return;
        }

        AttributeEvent event = null;

        // check whether it is multi-line or single-line update - if 5 is update it is multi-line
        if (UPDATE_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, 6))) {
            LOG.fine("Received multi-line attributes update for asset " + assetId);
        } else {
            LOG.fine("Received single-line attribute update for asset " + assetId);
        }



    }



    @Override
    public void onPublish(RemotingConnection connection, Topic topic, ByteBuf body) {
        if (!isGatewayConnection(connection)) {
            LOG.warning("Received message from non-gateway connection " + MQTTBrokerService.connectionToString(connection));
            return;
        }

        // we want to call the handlers based on the given topic, if no appropriate handler is found, we will log a warning
        String payloadContent = body.toString(StandardCharsets.UTF_8);
        LOG.fine("Received message on topic " + topic + " with payload " + payloadContent);

        if (isHealthTopic(topic)) {
            LOG.fine("Received health request from gateway " + connection.getClientID());
        }
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

    // update the status of the gateway asset if its client id attribute is linked to the MQTT connection
    protected void updateGatewayAssetStatusIfLinked(RemotingConnection connection, ConnectionStatus status) {
        GatewayV2Asset gatewayAsset = (GatewayV2Asset) assetStorageService.find(new AssetQuery()
                .types(GatewayV2Asset.class).attributeValue(GatewayV2Asset.CLIENT_ID.getName(), connection.getClientID()));

        if (gatewayAsset != null) {
            LOG.fine("Linked Gateway asset found for MQTT client, updating connection status to " + status);
            sendAttributeEvent(new AttributeEvent(gatewayAsset.getId(), GatewayAsset.STATUS, status));
        }
    }

    protected boolean isHealthTopic(Topic topic) {
        return HEALTH_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, HEALTH_TOKEN_INDEX));
    }

    protected boolean isAssetsTopic(Topic topic) {
        return ASSETS_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, ASSETS_TOKEN_INDEX));
    }

    protected boolean isAttributesTopic(Topic topic) {
        return ATTRIBUTES_TOPIC.equalsIgnoreCase(topicTokenIndexToString(topic, ATTRIBUTES_TOKEN_INDEX));
    }

    protected boolean isGatewayConnection(RemotingConnection connection) {
        return assetStorageService.find(new AssetQuery().types(GatewayV2Asset.class).attributeValue(GatewayV2Asset.CLIENT_ID.getName(), connection.getClientID())) != null;
    }

}
