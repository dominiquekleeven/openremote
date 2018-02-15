/*
 * Copyright 2017, OpenRemote Inc.
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
package org.openremote.manager.rules;

import org.jeasy.rules.core.InferenceRulesEngine;
import org.jeasy.rules.core.RulesEngineParameters;
import org.openremote.container.timer.TimerService;
import org.openremote.manager.asset.AssetProcessingService;
import org.openremote.manager.asset.AssetStorageService;
import org.openremote.manager.concurrent.ManagerExecutorService;
import org.openremote.manager.notification.NotificationService;
import org.openremote.manager.rules.facade.AssetsFacade;
import org.openremote.manager.rules.facade.UsersFacade;
import org.openremote.manager.security.ManagerIdentityService;
import org.openremote.model.rules.*;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.openremote.container.concurrent.GlobalLock.withLock;
import static org.openremote.manager.rules.RulesetDeployment.Status.*;

public class RulesEngine<T extends Ruleset> {

    public static final Logger LOG = Logger.getLogger(RulesEngine.class.getName());

    // Separate logger for execution of rules
    public static final Logger RULES_LOG = Logger.getLogger("org.openremote.rules.Rules");

    // Separate logger for periodic stats printer
    public static final Logger STATS_LOG = Logger.getLogger("org.openremote.rules.RulesEngineStats");

    final protected TimerService timerService;
    final protected ManagerExecutorService executorService;
    final protected AssetStorageService assetStorageService;

    final protected RulesEngineId<T> id;
    final protected Assets assetsFacade;
    final protected Users usersFacade;

    final protected Map<Long, RulesetDeployment> deployments = new LinkedHashMap<>();
    final protected RulesFacts facts;
    final protected InferenceRulesEngine engine;

    protected boolean running;
    protected ScheduledFuture eventsTimer;
    protected ScheduledFuture statsTimer;

    // Only used to optimize toString(), contains the details of this engine
    protected String deploymentInfo;

    // Only used in tests to prevent scheduled firing of engine
    protected boolean disableTemporaryFactExpiration = false;

    public RulesEngine(TimerService timerService,
                       ManagerIdentityService identityService,
                       ManagerExecutorService executorService,
                       AssetStorageService assetStorageService,
                       AssetProcessingService assetProcessingService,
                       NotificationService notificationService,
                       RulesEngineId<T> id) {
        this.timerService = timerService;
        this.executorService = executorService;
        this.assetStorageService = assetStorageService;
        this.id = id;
        this.assetsFacade = new AssetsFacade<>(id, assetStorageService, assetProcessingService::sendAttributeEvent);
        this.usersFacade = new UsersFacade<>(id, assetStorageService, notificationService, identityService);

        this.facts = new RulesFacts(assetsFacade, this, RULES_LOG);
        engine = new InferenceRulesEngine(
            // Skip any other rules after the first failed rule (exception thrown in condition or action)
            new RulesEngineParameters(false, true, false, RulesEngineParameters.DEFAULT_RULE_PRIORITY_THRESHOLD)
        );
        engine.registerRulesEngineListener(facts);
        engine.registerRuleListener(facts);
    }

    public RulesEngineId<T> getId() {
        return id;
    }

    /**
     * @return a shallow copy of the asset state facts.
     */
    public Set<AssetState> getAssetStates() {
        return new HashSet<>(facts.getAssetStates());
    }

    /**
     * @return a shallow copy of the asset event facts.
     */
    public List<TemporaryFact<AssetState>> getAssetEvents() {
        return new ArrayList<>(facts.getAssetEvents());
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isError() {
        for (RulesetDeployment deployment : deployments.values()) {
            if (deployment.status == COMPILATION_ERROR || deployment.getStatus() == EXECUTION_ERROR) {
                return true;
            }
        }
        return false;
    }

    public RuntimeException getError() {
        List<RulesetDeployment> deploymentsWithCompilationError = new ArrayList<>();
        List<RulesetDeployment> deploymentsWithExecutionError = new ArrayList<>();
        for (RulesetDeployment deployment : deployments.values()) {
            if (deployment.getStatus() == COMPILATION_ERROR) {
                deploymentsWithCompilationError.add(deployment);
            } else if (deployment.getStatus() == EXECUTION_ERROR) {
                deploymentsWithExecutionError.add(deployment);
            }
        }
        if (deploymentsWithCompilationError.size() > 0 || deploymentsWithExecutionError.size() > 0) {
            return new RuntimeException(
                "Ruleset deployments have errors, failed compilation: "
                    + deploymentsWithCompilationError.size()
                    + ", failed execution: "
                    + deploymentsWithExecutionError.size() + " - on: " + this
            );
        }
        return null;
    }

    /**
     * @return <code>true</code> if all rulesets are {@link RulesetDeployment.Status#DEPLOYED} and this engine can be started.
     */
    public boolean isDeployed() {
        return deployments.values().stream().allMatch(rd -> rd.getStatus() == DEPLOYED);
    }

    public void addRuleset(T ruleset) {
        if (ruleset == null || ruleset.getRules() == null || ruleset.getRules().isEmpty()) {
            // Assume it's a success if deploying an empty ruleset
            LOG.finest("Ruleset is empty so no rules to deploy");
            return;
        }

        RulesetDeployment deployment = deployments.get(ruleset.getId());

        stop();

        // Check if ruleset is already deployed (maybe an older version)
        if (deployment != null) {
            LOG.info("Removing ruleset deployment: " + ruleset);
            deployments.remove(ruleset.getId());
            updateDeploymentInfo();
        }

        deployment = new RulesetDeployment(ruleset.getId(), ruleset.getName(), ruleset.getVersion());

        boolean compilationSuccessful = deployment.registerRules(ruleset, assetsFacade, usersFacade);

        if (!compilationSuccessful) {
            // If any other ruleset is DEPLOYED in this scope, demote to READY
            for (RulesetDeployment rd : deployments.values()) {
                if (rd.getStatus() == DEPLOYED) {
                    rd.setStatus(READY);
                }
            }
        }

        // Add new ruleset and set its status to either DEPLOYED or COMPILATION_ERROR
        deployment.setStatus(compilationSuccessful ? DEPLOYED : COMPILATION_ERROR);
        deployments.put(ruleset.getId(), deployment);
        updateDeploymentInfo();

        start();
    }

    /**
     * @return <code>true</code> if this rules engine has no deployments.
     */
    public boolean removeRuleset(Ruleset ruleset) {
        if (!deployments.containsKey(ruleset.getId())) {
            LOG.finer("Ruleset cannot be retracted as it was never deployed: " + ruleset);
            return deployments.size() == 0;
        }

        stop();

        deployments.remove(ruleset.getId());
        updateDeploymentInfo();

        // If there are no deployments with COMPILATION_ERROR, promote all which are READY to DEPLOYED
        boolean anyDeploymentsHaveCompilationError = deployments
            .values()
            .stream()
            .anyMatch(rd -> rd.getStatus() == COMPILATION_ERROR);

        if (!anyDeploymentsHaveCompilationError) {
            deployments.values().forEach(rd -> {
                if (rd.getStatus() == READY) {
                    rd.setStatus(DEPLOYED);
                }
            });
        }

        if (deployments.size() > 0) {
            start();
            return false;
        } else {
            return true;
        }
    }

    public void start() {
        if (isRunning()) {
            return;
        }

        if (!isDeployed()) {
            LOG.fine("Cannot start rules engine, not all rulesets are status " + DEPLOYED);
            return;
        }

        if (deployments.size() == 0) {
            LOG.finest("No rulesets so nothing to start");
            return;
        }

        LOG.info("Starting: " + this);
        running = true;
        fire();

        // Start a background stats printer if INFO level logging is enabled
        if (STATS_LOG.isLoggable(Level.INFO) || STATS_LOG.isLoggable(Level.FINEST)) {
            if (STATS_LOG.isLoggable(Level.FINEST)) {
                LOG.info("On " + this + ", enabling periodic statistics output at INFO level every 30 seconds on category: " + STATS_LOG.getName());
            } else {
                LOG.info("On " + this + ", enabling periodic full memory dump at FINEST level every 30 seconds on category: " + STATS_LOG.getName());
            }
            statsTimer = executorService.scheduleAtFixedRate(this::printSessionStats, 3, 30, TimeUnit.SECONDS);
        }
    }

    public void fire() {
        // Submit a task that fires the engine for all deployments
        executorService.submit(() -> withLock(getClass().getSimpleName(), () -> {
            if (!running) {
                return;
            }

            // Set the current clock
            RulesClock clock = new RulesClock(timerService);
            facts.setClock(clock);

            // Remove any expired temporary facts
            boolean factsExpired = facts.removeExpiredTemporaryFacts();

            // TODO Optimize if scheduled firing of rules becomes a performance problem:
            // Add fire(scheduledFiring) boolean
            // Add enableTimer() as rule declaration option, each rule which uses time windows must set it
            // Only fire if factsExpired || deployment.isAnyRuleEnabledTimer()
            // Rules could default to enableTimer(true) and they could reduce firing frequency with enableTimer("1m")

            for (RulesetDeployment deployment : deployments.values()) {
                try {
                    RULES_LOG.fine("Firing rules @" + clock + " of: " + deployment);

                    // If full detail logging is enabled
                    if (RULES_LOG.isLoggable(Level.FINEST)) {
                        // Log asset states and events before firing (note that this will log at INFO)
                        facts.logFacts(RULES_LOG);
                    }

                    engine.fire(deployment.getRules(), facts);

                } catch (Exception ex) {
                    LOG.log(Level.SEVERE, "On " + RulesEngine.this + ", error firing rules of: " + deployment, ex);

                    deployment.setStatus(EXECUTION_ERROR);
                    deployment.setError(ex);

                    // TODO We always stop on any error, good idea?
                    // TODO We only get here on LHS runtime errors, RHS runtime errors are in RuleFacts.onFailure()
                    stop();

                    // TODO We skip any other deployment when we hit the first error, good idea?
                    break;
                }
            }

            // If we still have temporary facts, schedule a new firing so expired facts are removed eventually
            if (facts.hasTemporaryFacts() && eventsTimer == null && !disableTemporaryFactExpiration) {
                LOG.fine("Temporary facts present, scheduling new firing of rules on: " + this);
                eventsTimer = executorService.scheduleAtFixedRate(
                    this::fire,
                    TemporaryFact.GUARANTEED_MIN_EXPIRATION_MILLIS,
                    TemporaryFact.GUARANTEED_MIN_EXPIRATION_MILLIS,
                    TimeUnit.MILLISECONDS
                );
            } else if (!facts.hasTemporaryFacts() && eventsTimer != null) {
                LOG.fine("No temporary facts present, cancel scheduled firing of rules on: " + this);
                eventsTimer.cancel(false);
                eventsTimer = null;
            }
        }));
    }

    public void stop() {
        if (!isRunning()) {
            return;
        }
        LOG.info("Stopping: " + this);
        if (eventsTimer != null) {
            eventsTimer.cancel(true);
            eventsTimer = null;
        }
        if (statsTimer != null) {
            statsTimer.cancel(true);
            statsTimer = null;
        }
        running = false;
    }

    public void updateFact(AssetState assetState, boolean fireImmediately) {
        facts.putAssetState(assetState);
        if (fireImmediately) {
            fire();
        }
    }

    public void removeFact(AssetState assetState) {
        facts.removeAssetState(assetState);
        fire();
    }

    public void insertFact(String expires, AssetState assetState) {
        facts.insertAssetEvent(expires, assetState);
        fire();
    }

    /* TODO
        protected String compileTemplate(String templateAssetId, String rules) {
            ServerAsset templateAsset = assetStorageService.find(templateAssetId, true);

            if (templateAsset == null)
                throw new IllegalStateException("Template asset not found: " + templateAssetId);

            List<TemplateFilter> filters = templateAsset.getAttributesStream()
                .filter(isAttributeTypeEqualTo(AttributeType.RULES_TEMPLATE_FILTER))
                .map(attribute -> new Pair<>(attribute.getName(), attribute.getValue()))
                .filter(pair -> pair.key.isPresent() && pair.value.isPresent())
                .map(pair -> new Pair<>(pair.key.get(), pair.value.get()))
                .map(pair -> TemplateFilter.fromModelValue(pair.key, pair.value))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

            LOG.fine("Rendering rules template with filters: " + filters);

            ObjectDataCompiler converter = new ObjectDataCompiler();
            InputStream is = new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8));
            return converter.compile(filters, is);
        }
    */

    protected void updateDeploymentInfo() {
        deploymentInfo = Arrays.toString(
            deployments.values().stream()
                .map(RulesetDeployment::toString)
                .toArray(String[]::new)
        );
    }

    protected void printSessionStats() {
        withLock(getClass().getSimpleName(), () -> {
            Collection<AssetState> assetStateFacts = facts.getAssetStates();
            Collection<TemporaryFact<AssetState>> assetEventFacts = facts.getAssetEvents();
            Map<String, Object> namedFacts = facts.getNamedFacts();
            Collection<Object> anonFacts = facts.getAnonymousFacts();
            long total = assetStateFacts.size() + assetEventFacts.size() + namedFacts.size() + anonFacts.size();
            STATS_LOG.info("On " + this + ", in memory facts are Total: " + total
                + ", AssetState: " + assetStateFacts.size()
                + ", AssetEvent: " + assetEventFacts.size()
                + ", Named: " + namedFacts.size()
                + ", Anonymous: " + anonFacts.size());

            // Additional details if FINEST is enabled
            if (STATS_LOG.isLoggable(Level.FINEST)) {
                facts.logFacts(STATS_LOG);
            }
        });
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "id='" + id + '\'' +
            ", running='" + isRunning() + '\'' +
            ", deployments='" + deploymentInfo + '\'' +
            '}';
    }
}
