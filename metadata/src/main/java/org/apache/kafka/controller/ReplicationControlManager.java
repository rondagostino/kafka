/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.BrokersToIsrs.TopicPartition;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;


/**
 * The ReplicationControlManager is the part of the controller which deals with topics
 * and partitions. It is responsible for managing the in-sync replica set and leader
 * of each partition, as well as administrative tasks like creating or deleting topics.
 */
public class ReplicationControlManager {
    static class TopicControlInfo {
        private final Uuid id;
        private final TimelineHashMap<Integer, PartitionControlInfo> parts;

        TopicControlInfo(SnapshotRegistry snapshotRegistry, Uuid id) {
            this.id = id;
            this.parts = new TimelineHashMap<>(snapshotRegistry, 0);
        }
    }

    static class PartitionControlInfo {
        private final int[] replicas;
        private final int[] isr;
        private final int[] removingReplicas;
        private final int[] addingReplicas;
        private final int leader;
        private final int leaderEpoch;
        private final int partitionEpoch;

        PartitionControlInfo(PartitionRecord record) {
            this(Replicas.toArray(record.replicas()),
                Replicas.toArray(record.isr()),
                Replicas.toArray(record.removingReplicas()),
                Replicas.toArray(record.addingReplicas()),
                record.leader(),
                record.leaderEpoch(),
                record.partitionEpoch());
        }

        PartitionControlInfo(int[] replicas, int[] isr, int[] removingReplicas,
                             int[] addingReplicas, int leader, int leaderEpoch,
                             int partitionEpoch) {
            this.replicas = replicas;
            this.isr = isr;
            this.removingReplicas = removingReplicas;
            this.addingReplicas = addingReplicas;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.partitionEpoch = partitionEpoch;
        }

        PartitionControlInfo merge(PartitionChangeRecord record) {
            return new PartitionControlInfo(replicas,
                Replicas.toArray(record.isr()),
                removingReplicas,
                addingReplicas,
                record.leader(),
                record.leaderEpoch(),
                record.partitionEpoch());
        }

        String diff(PartitionControlInfo prev) {
            StringBuilder builder = new StringBuilder();
            String prefix = "";
            if (!Arrays.equals(replicas, prev.replicas)) {
                builder.append(prefix).append("oldReplicas=").append(Arrays.toString(prev.replicas));
                prefix = ", ";
                builder.append(prefix).append("newReplicas=").append(Arrays.toString(replicas));
            }
            if (!Arrays.equals(isr, prev.isr)) {
                builder.append(prefix).append("oldIsr=").append(Arrays.toString(prev.isr));
                prefix = ", ";
                builder.append(prefix).append("newIsr=").append(Arrays.toString(isr));
            }
            if (!Arrays.equals(removingReplicas, prev.removingReplicas)) {
                builder.append(prefix).append("oldRemovingReplicas=").
                    append(Arrays.toString(prev.removingReplicas));
                prefix = ", ";
                builder.append(prefix).append("newRemovingReplicas=").
                    append(Arrays.toString(removingReplicas));
            }
            if (!Arrays.equals(addingReplicas, prev.addingReplicas)) {
                builder.append(prefix).append("oldAddingReplicas=").
                    append(Arrays.toString(prev.addingReplicas));
                prefix = ", ";
                builder.append(prefix).append("newAddingReplicas=").
                    append(Arrays.toString(addingReplicas));
            }
            if (leader != prev.leader) {
                builder.append(prefix).append("oldLeader=").append(prev.leader);
                prefix = ", ";
                builder.append(prefix).append("newLeader=").append(leader);
            }
            if (leaderEpoch != prev.leaderEpoch) {
                builder.append(prefix).append("oldLeaderEpoch=").append(prev.leaderEpoch);
                prefix = ", ";
                builder.append(prefix).append("newLeaderEpoch=").append(leaderEpoch);
            }
            if (partitionEpoch != prev.partitionEpoch) {
                builder.append(prefix).append("oldPartitionEpoch=").append(prev.partitionEpoch);
                prefix = ", ";
                builder.append(prefix).append("newPartitionEpoch=").append(partitionEpoch);
            }
            return builder.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isr, removingReplicas, addingReplicas, leader,
                leaderEpoch, partitionEpoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionControlInfo)) return false;
            PartitionControlInfo other = (PartitionControlInfo) o;
            return diff(other).isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("PartitionControlInfo(");
            builder.append("replicas=").append(Arrays.toString(replicas));
            builder.append(", isr=").append(Arrays.toString(isr));
            builder.append(", removingReplicas=").append(Arrays.toString(removingReplicas));
            builder.append(", addingReplicas=").append(Arrays.toString(addingReplicas));
            builder.append(", leader=").append(leader);
            builder.append(", leaderEpoch=").append(leaderEpoch);
            builder.append(", partitionEpoch=").append(partitionEpoch);
            builder.append(")");
            return builder.toString();
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;

    /**
     * The random number generator used by this object.
     */
    private final Random random;

    /**
     * The KIP-464 default replication factor that is used if a CreateTopics request does
     * not specify one.
     */
    private final short defaultReplicationFactor;

    /**
     * The KIP-464 default number of partitions that is used if a CreateTopics request does
     * not specify a number of partitions.
     */
    private final int defaultNumPartitions;

    /**
     * A reference to the controller's configuration control manager.
     */
    private final ConfigurationControlManager configurationControl;

    /**
     * A reference to the controller's cluster control manager.
     */
    final ClusterControlManager clusterControl;

    /**
     * Maps topic names to topic UUIDs.
     */
    private final TimelineHashMap<String, Uuid> topicsByName;

    /**
     * Maps topic UUIDs to structures containing topic information, including partitions.
     */
    private final TimelineHashMap<Uuid, TopicControlInfo> topics;

    /**
     * A map of broker IDs to the partitions that the broker is in the ISR for.
     */
    private final BrokersToIsrs brokersToIsrs;

    ReplicationControlManager(SnapshotRegistry snapshotRegistry,
                              LogContext logContext,
                              Random random,
                              short defaultReplicationFactor,
                              int defaultNumPartitions,
                              ConfigurationControlManager configurationControl,
                              ClusterControlManager clusterControl) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(ReplicationControlManager.class);
        this.random = random;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.defaultNumPartitions = defaultNumPartitions;
        this.configurationControl = configurationControl;
        this.clusterControl = clusterControl;
        this.topicsByName = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
    }

    public void replay(TopicRecord record) {
        topicsByName.put(record.name(), record.topicId());
        topics.put(record.topicId(), new TopicControlInfo(snapshotRegistry, record.topicId()));
        log.info("Created topic {} with ID {}.", record.name(), record.topicId());
    }

    public void replay(PartitionRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo newPartInfo = new PartitionControlInfo(record);
        PartitionControlInfo prevPartInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartInfo == null) {
            log.info("Created partition {}:{} with {}.", record.topicId(),
                record.partitionId(), newPartInfo.toString());
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            brokersToIsrs.update(record.topicId(), record.partitionId(), null,
                newPartInfo.isr, -1, newPartInfo.leader);
        } else {
            String diff = newPartInfo.diff(prevPartInfo);
            if (!diff.isEmpty()) {
                log.info("Modified partition {}:{}: {}.", record.topicId(),
                    record.partitionId(), diff);
                topicInfo.parts.put(record.partitionId(), newPartInfo);
                brokersToIsrs.update(record.topicId(), record.partitionId(),
                    prevPartInfo.isr, newPartInfo.isr, prevPartInfo.leader,
                    newPartInfo.leader);
            }
        }
    }

    public void replay(PartitionChangeRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo prevPartitionInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartitionInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no partition with that id was found.");
        }
        PartitionControlInfo newPartitionInfo = prevPartitionInfo.merge(record);
        topicInfo.parts.put(record.partitionId(), newPartitionInfo);
        brokersToIsrs.update(record.topicId(), record.partitionId(),
            prevPartitionInfo.isr, newPartitionInfo.isr, prevPartitionInfo.leader,
            newPartitionInfo.leader);
        log.debug("Applied ISR change record: {}", record.toString());
    }

    ControllerResult<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check the topic names.
        validateNewTopicNames(topicErrors, request.topics());

        // Identify topics that already exist and mark them with the appropriate error
        request.topics().stream().filter(creatableTopic -> topicsByName.containsKey(creatableTopic.name()))
                .forEach(t -> topicErrors.put(t.name(), new ApiError(Errors.TOPIC_ALREADY_EXISTS)));

        // Verify that the configurations for the new topics are OK, and figure out what
        // ConfigRecords should be created.
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges =
            computeConfigChanges(topicErrors, request.topics());
        ControllerResult<Map<ConfigResource, ApiError>> configResult =
            configurationControl.incrementalAlterConfigs(configChanges);
        for (Entry<ConfigResource, ApiError> entry : configResult.response().entrySet()) {
            if (entry.getValue().isFailure()) {
                topicErrors.put(entry.getKey().name(), entry.getValue());
            }
        }
        records.addAll(configResult.records());

        // Try to create whatever topics are needed.
        Map<String, CreatableTopicResult> successes = new HashMap<>();
        for (CreatableTopic topic : request.topics()) {
            if (topicErrors.containsKey(topic.name())) continue;
            ApiError error = createTopic(topic, records, successes);
            if (error.isFailure()) {
                topicErrors.put(topic.name(), error);
            }
        }

        // Create responses for all topics.
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        StringBuilder resultsBuilder = new StringBuilder();
        String resultsPrefix = "";
        for (CreatableTopic topic : request.topics()) {
            ApiError error = topicErrors.get(topic.name());
            if (error != null) {
                data.topics().add(new CreatableTopicResult().
                    setName(topic.name()).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
                resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                    append(error.error()).append(" (").append(error.message()).append(")");
                resultsPrefix = ", ";
                continue;
            }
            CreatableTopicResult result = successes.get(topic.name());
            data.topics().add(result);
            resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                append("SUCCESS");
            resultsPrefix = ", ";
        }
        log.info("createTopics result(s): {}", resultsBuilder.toString());
        return new ControllerResult<>(records, data);
    }

    private ApiError createTopic(CreatableTopic topic,
                                 List<ApiMessageAndVersion> records,
                                 Map<String, CreatableTopicResult> successes) {
        Map<Integer, PartitionControlInfo> newParts = new HashMap<>();
        if (!topic.assignments().isEmpty()) {
            if (topic.replicationFactor() != -1) {
                return new ApiError(Errors.INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication " +
                    "factor was not set to -1.");
            }
            if (topic.numPartitions() != -1) {
                return new ApiError(Errors.INVALID_REQUEST,
                    "A manual partition assignment was specified, but numPartitions " +
                        "was not set to -1.");
            }
            for (CreatableReplicaAssignment assignment : topic.assignments()) {
                if (newParts.containsKey(assignment.partitionIndex())) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                        "Found multiple manual partition assignments for partition " +
                            assignment.partitionIndex());
                }
                HashSet<Integer> brokerIds = new HashSet<>();
                for (int brokerId : assignment.brokerIds()) {
                    if (!brokerIds.add(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment specifies the same node " +
                                "id more than once.");
                    } else if (!clusterControl.unfenced(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment contains node " + brokerId +
                                ", but that node is not usable.");
                    }
                }
                int[] replicas = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    replicas[i] = assignment.brokerIds().get(i);
                }
                int[] isr = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    isr[i] = assignment.brokerIds().get(i);
                }
                newParts.put(assignment.partitionIndex(),
                    new PartitionControlInfo(replicas, isr, null, null, isr[0], 0, 0));
            }
        } else if (topic.replicationFactor() < -1 || topic.replicationFactor() == 0) {
            return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                "Replication factor was set to an invalid non-positive value.");
        } else if (!topic.assignments().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST,
                "Replication factor was not set to -1 but a manual partition " +
                    "assignment was specified.");
        } else if (topic.numPartitions() < -1 || topic.numPartitions() == 0) {
            return new ApiError(Errors.INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.");
        } else {
            int numPartitions = topic.numPartitions() == -1 ?
                defaultNumPartitions : topic.numPartitions();
            short replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            try {
                List<List<Integer>> replicas = clusterControl.
                    placeReplicas(numPartitions, replicationFactor);
                for (int partitionId = 0; partitionId < replicas.size(); partitionId++) {
                    int[] r = Replicas.toArray(replicas.get(partitionId));
                    newParts.put(partitionId,
                        new PartitionControlInfo(r, r, null, null, r[0], 0, 0));
                }
            } catch (InvalidReplicationFactorException e) {
                return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                    "Unable to replicate the partition " + replicationFactor +
                        " times: " + e.getMessage());
            }
        }
        Uuid topicId = new Uuid(random.nextLong(), random.nextLong());
        successes.put(topic.name(), new CreatableTopicResult().
            setName(topic.name()).
            setTopicId(topicId).
            setErrorCode((short) 0).
            setErrorMessage(null).
            setNumPartitions(newParts.size()).
            setReplicationFactor((short) newParts.get(0).replicas.length));
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        for (Entry<Integer, PartitionControlInfo> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionControlInfo info = partEntry.getValue();
            records.add(new ApiMessageAndVersion(new PartitionRecord().
                setPartitionId(partitionIndex).
                setTopicId(topicId).
                setReplicas(Replicas.toList(info.replicas)).
                setIsr(Replicas.toList(info.isr)).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(info.leader).
                setLeaderEpoch(info.leaderEpoch).
                setPartitionEpoch(0), (short) 0));
        }
        return ApiError.NONE;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (InvalidTopicException e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_TOPIC_EXCEPTION, e.getMessage()));
            }
        }
    }

    static Map<ConfigResource, Map<String, Entry<OpType, String>>>
            computeConfigChanges(Map<String, ApiError> topicErrors,
                                 CreatableTopicCollection topics) {
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges = new HashMap<>();
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            Map<String, Entry<OpType, String>> topicConfigs = new HashMap<>();
            for (CreateTopicsRequestData.CreateableTopicConfig config : topic.configs()) {
                topicConfigs.put(config.name(), new SimpleImmutableEntry<>(SET, config.value()));
            }
            if (!topicConfigs.isEmpty()) {
                configChanges.put(new ConfigResource(TOPIC, topic.name()), topicConfigs);
            }
        }
        return configChanges;
    }

    // VisibleForTesting
    PartitionControlInfo getPartition(Uuid topicId, int partitionId) {
        TopicControlInfo topic = topics.get(topicId);
        if (topic == null) {
            return null;
        }
        return topic.parts.get(partitionId);
    }

    // VisibleForTesting
    BrokersToIsrs brokersToIsrs() {
        return brokersToIsrs;
    }

    ControllerResult<AlterIsrResponseData> alterIsr(AlterIsrRequestData request) {
        clusterControl.checkBrokerEpoch(request.brokerId(), request.brokerEpoch());
        AlterIsrResponseData response = new AlterIsrResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (AlterIsrRequestData.TopicData topicData : request.topics()) {
            AlterIsrResponseData.TopicData responseTopicData =
                new AlterIsrResponseData.TopicData().setName(topicData.name());
            response.topics().add(responseTopicData);
            Uuid topicId = topicsByName.get(topicData.name());
            if (topicId == null || !topics.containsKey(topicId)) {
                for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                continue;
            }
            TopicControlInfo topic = topics.get(topicId);
            for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                PartitionControlInfo partition = topic.parts.get(partitionData.partitionIndex());
                if (partition == null) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                    continue;
                }
                if (partitionData.leaderEpoch() != partition.leaderEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.FENCED_LEADER_EPOCH.code()));
                    continue;
                }
                if (partitionData.currentIsrVersion() != partition.partitionEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_UPDATE_VERSION.code()));
                    continue;
                }
                int[] newIsr = Replicas.toArray(partitionData.newIsr());
                if (!Replicas.validateIsr(partition.replicas, newIsr)) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_REQUEST.code()));
                }
                if (!Replicas.contains(newIsr, partition.leader)) {
                    // An alterIsr request can't remove the current leader.
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_REQUEST.code()));
                }
                records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                    setPartitionId(partitionData.partitionIndex()).
                    setTopicId(topic.id).
                    setIsr(partitionData.newIsr()).
                    setLeader(partition.leader).
                    setLeaderEpoch(partition.leaderEpoch).
                    setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
            }
        }
        return new ControllerResult<>(records, response);
    }

    /**
     * Generate the appropriate records to handle a broker being fenced.
     *
     * First, we remove this broker from any non-singleton ISR. Then we generate a
     * FenceBrokerRecord.
     *
     * @param brokerId      The broker id.
     * @param records       The record list to append to.
     */

    void handleBrokerFenced(int brokerId, List<ApiMessageAndVersion> records) {
        BrokerRegistration brokerRegistration = clusterControl.brokerRegistrations().get(brokerId);
        if (brokerRegistration == null) {
            throw new RuntimeException("Can't find broker registration for broker " + brokerId);
        }
        handleNodeDeactivated(brokerId, records);
        records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
            setId(brokerId).setEpoch(brokerRegistration.epoch()), (short) 0));
    }

    /**
     * Generate the appropriate records to handle a broker being unregistered.
     *
     * First, we remove this broker from any non-singleton ISR. Then we generate a
     * FenceBrokerRecord.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnregistered(int brokerId, long brokerEpoch,
                                  List<ApiMessageAndVersion> records) {
        handleNodeDeactivated(brokerId, records);
        records.add(new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(brokerId).setBrokerEpoch(brokerEpoch), (short) 0));
    }

    /**
     * Handle a broker being deactivated. This means we remove it from any ISR that has
     * more than one element. We do not remove the broker from ISRs where it is the only
     * member since this would preclude clean leader election in the future.
     *
     * @param brokerId      The broker id.
     * @param records       The record list to append to.
     */
    void handleNodeDeactivated(int brokerId, List<ApiMessageAndVersion> records) {
        Iterator<TopicPartition> iterator = brokersToIsrs.iterator(brokerId, false);
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            TopicControlInfo topic = topics.get(topicPartition.topicId());
            if (topic == null) {
                throw new RuntimeException("Topic ID " + topicPartition.topicId() + " existed in " +
                    "isrMembers, but not in the topics map.");
            }
            PartitionControlInfo partition = topic.parts.get(topicPartition.partitionId());
            if (partition == null) {
                throw new RuntimeException("Partition " + topicPartition +
                    " existed in isrMembers, but not in the partitions map.");
            }
            int[] newIsr = Replicas.copyWithout(partition.isr, brokerId);
            int newLeader, newLeaderEpoch;
            if (newIsr.length == 0) {
                // We don't want to shrink the ISR to size 0. So, leave the node in the
                // ISR, but set the leader to -1 (no leader).
                newIsr = partition.isr;
                newLeader = -1;
                newLeaderEpoch = partition.leaderEpoch + 1;
            } else if (partition.leader == brokerId) {
                // The fenced node will no longer be the leader.
                newLeader = chooseNewLeader(partition, newIsr, false);
                newLeaderEpoch = partition.leaderEpoch + 1;
            } else {
                // The fenced node wasn't the leader, so no leader change is needed.
                newLeader = partition.leader;
                newLeaderEpoch = partition.leaderEpoch;
            }
            records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                setPartitionId(topicPartition.partitionId()).
                setTopicId(topic.id).
                setIsr(Replicas.toList(newIsr)).
                setLeader(newLeader).
                setLeaderEpoch(newLeaderEpoch).
                setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
        }
    }

    /**
     * Generate the appropriate records to handle a broker becoming unfenced.
     *
     * First, we create an UnfenceBrokerRecord. Then, we check if if there are any
     * partitions that don't currently have a leader that should be led by the newly
     * unfenced broker.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnfenced(int brokerId, long brokerEpoch, List<ApiMessageAndVersion> records) {
        records.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
            setId(brokerId).setEpoch(brokerEpoch), (short) 0));
        handleNodeActivated(brokerId, records);
    }

    /**
     * Handle a broker being activated. This means we check if it can become the leader
     * for any partition that currently has no leader (aka offline partition).
     *
     * @param brokerId      The broker id.
     * @param records       The record list to append to.
     */
    void handleNodeActivated(int brokerId, List<ApiMessageAndVersion> records) {
        Iterator<TopicPartition> iterator = brokersToIsrs.noLeaderIterator();
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            TopicControlInfo topic = topics.get(topicPartition.topicId());
            if (topic == null) {
                throw new RuntimeException("Topic ID " + topicPartition.topicId() + " existed in " +
                    "isrMembers, but not in the topics map.");
            }
            PartitionControlInfo partition = topic.parts.get(topicPartition.partitionId());
            if (partition == null) {
                throw new RuntimeException("Partition " + topicPartition +
                    " existed in isrMembers, but not in the partitions map.");
            }
            // TODO: if this partition is configured for unclean leader election,
            // check the replica set rather than the ISR.
            if (Replicas.contains(partition.isr, brokerId)) {
                records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                    setPartitionId(topicPartition.partitionId()).
                    setTopicId(topic.id).
                    setIsr(Replicas.toList(partition.isr)).
                    setLeader(brokerId).
                    setLeaderEpoch(partition.leaderEpoch + 1).
                    setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
            }
        }
    }

    void removeLeaderships(int brokerId, List<ApiMessageAndVersion> records) {
        Iterator<TopicPartition> iterator = brokersToIsrs.iterator(brokerId, true);
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            TopicControlInfo topic = topics.get(topicPartition.topicId());
            if (topic == null) {
                throw new RuntimeException("Topic ID " + topicPartition.topicId() + " existed in " +
                    "isrMembers, but not in the topics map.");
            }
            PartitionControlInfo partition = topic.parts.get(topicPartition.partitionId());
            if (partition == null) {
                throw new RuntimeException("Partition " + topicPartition +
                    " existed in isrMembers, but not in the partitions map.");
            }
            int[] isrWithoutCurLeader = Replicas.copyWithout(partition.isr, brokerId);
            int newLeader = chooseNewLeader(partition, isrWithoutCurLeader, false);
            records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                setPartitionId(topicPartition.partitionId()).
                setTopicId(topic.id).
                setIsr(Replicas.toList(partition.isr)).
                setLeader(newLeader).
                setLeaderEpoch(partition.leaderEpoch + 1).
                setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
        }
    }

    ControllerResult<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
        boolean unclean = electionIsUnclean(request.electionType());
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ElectLeadersResponseData response = new ElectLeadersResponseData();
        for (TopicPartitions topic : request.topicPartitions()) {
            ReplicaElectionResult topicResults =
                new ReplicaElectionResult().setTopic(topic.topic());
            response.replicaElectionResults().add(topicResults);
            for (int partitionId : topic.partitions()) {
                ApiError error = electLeader(topic.topic(), partitionId, unclean, records);
                topicResults.partitionResult().add(new PartitionResult().
                    setPartitionId(partitionId).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
            }
        }
        return new ControllerResult<>(records, response);
    }

    static boolean electionIsUnclean(byte electionType) {
        ElectionType type;
        try {
            type = ElectionType.valueOf(electionType);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Unknown election type " + (int) electionType);
        }
        return type == ElectionType.UNCLEAN;
    }

    ApiError electLeader(String topic, int partitionId, boolean unclean,
                         List<ApiMessageAndVersion> records) {
        Uuid topicId = topicsByName.get(topic);
        if (topicId == null) {
            return new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "No such topic as " + topic);
        }
        TopicControlInfo topicInfo = topics.get(topicId);
        if (topicInfo == null) {
            return new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "No such topic id as " + topicId);
        }
        PartitionControlInfo partitionInfo = topicInfo.parts.get(partitionId);
        if (partitionInfo == null) {
            return new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                "No such partition as " + topic + "-" + partitionId);
        }
        int newLeader = chooseNewLeader(partitionInfo, partitionInfo.isr, unclean);
        if (newLeader == partitionInfo.leader) {
            if (newLeader < 0) {
                return new ApiError(Errors.LEADER_NOT_AVAILABLE,
                    "Unable to find any leader for the partition.");
            } else {
                return ApiError.NONE;
            }
        } else {
            int[] newIsr = partitionInfo.isr;
            if (!Replicas.contains(partitionInfo.isr, newLeader)) {
                newIsr = new int[] {newLeader};
            }
            records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                setPartitionId(partitionId).
                setTopicId(topicId).
                setIsr(Replicas.toList(newIsr)).
                setLeader(newLeader).
                setLeaderEpoch(partitionInfo.leaderEpoch + 1).
                setPartitionEpoch(partitionInfo.partitionEpoch + 1), (short) 0));
            return ApiError.NONE;
        }
    }

    int chooseNewLeader(PartitionControlInfo partition, int[] newIsr, boolean unclean) {
        for (int i = 0; i < partition.replicas.length; i++) {
            int replica = partition.replicas[i];
            if (Replicas.contains(newIsr, replica)) {
                return replica;
            }
        }
        if (unclean) {
            for (int i = 0; i < partition.replicas.length; i++) {
                int replica = partition.replicas[i];
                if (clusterControl.unfenced(replica)) {
                    return replica;
                }
            }
        }
        return -1;
    }

    ControllerResult<BrokerHeartbeatReply> processBrokerHeartbeat(
                BrokerHeartbeatRequestData request, long lastCommittedOffset) {
        int brokerId = request.brokerId();
        long brokerEpoch = request.brokerEpoch();
        clusterControl.checkBrokerEpoch(brokerId, brokerEpoch);
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        BrokerControlStates states = heartbeatManager.calculateNextBrokerState(brokerId,
            request, lastCommittedOffset, brokersToIsrs.hasLeaderships(brokerId));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        if (states.current() != states.next()) {
            switch (states.next()) {
                case FENCED:
                    handleBrokerFenced(brokerId, records);
                    break;
                case UNFENCED:
                    handleBrokerUnfenced(brokerId, brokerEpoch, records);
                    break;
                case CONTROLLED_SHUTDOWN:
                    removeLeaderships(brokerId, records);
                    break;
                case SHUTDOWN_NOW:
                    handleBrokerFenced(brokerId, records);
                    break;
            }
        }
        heartbeatManager.touch(brokerId,
            states.next().fenced(),
            request.currentMetadataOffset());
        boolean isCaughtUp = request.currentMetadataOffset() >= lastCommittedOffset;
        BrokerHeartbeatReply reply = new BrokerHeartbeatReply(isCaughtUp,
                states.next().fenced(),
                states.next().inControlledShutdown(),
                states.next().shouldShutDown());
        return new ControllerResult<>(records, reply);
    }

    public ControllerResult<Void> unregisterBroker(int brokerId) {
        BrokerRegistration registration = clusterControl.brokerRegistrations().get(brokerId);
        if (registration == null) {
            throw new BrokerIdNotRegisteredException("Broker ID " + brokerId +
                " is not currently registered");
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        handleBrokerUnregistered(brokerId, registration.epoch(), records);
        return new ControllerResult<>(records, null);
    }

    ControllerResult<Void> maybeFenceStaleBrokers() {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        List<Integer> staleBrokers = heartbeatManager.findStaleBrokers();
        for (int brokerId : staleBrokers) {
            log.info("Fencing broker {} because its session has timed out.", brokerId);
            handleBrokerFenced(brokerId, records);
            heartbeatManager.fence(brokerId);
        }
        return new ControllerResult<>(records, null);
    }
}