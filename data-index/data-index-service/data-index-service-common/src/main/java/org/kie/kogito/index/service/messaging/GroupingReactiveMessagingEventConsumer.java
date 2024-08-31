/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kie.kogito.index.service.messaging;

import java.util.Collection;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.kie.kogito.event.DataEvent;
import org.kie.kogito.event.process.ProcessDefinitionDataEvent;
import org.kie.kogito.event.process.ProcessInstanceDataEvent;
import org.kie.kogito.event.usertask.UserTaskInstanceDataEvent;
import org.kie.kogito.index.event.KogitoJobCloudEvent;
import org.kie.kogito.index.service.IndexingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

@ApplicationScoped
@IfBuildProperty(name = "kogito.events.grouping", stringValue = "true")
public class GroupingReactiveMessagingEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingReactiveMessagingEventConsumer.class);

    public static final String KOGITO_PROCESSINSTANCES_EVENTS = "kogito-processinstances-events";
    public static final String KOGITO_PROCESS_DEFINITIONS_EVENTS = "kogito-processdefinitions-events";
    public static final String KOGITO_USERTASKINSTANCES_EVENTS = "kogito-usertaskinstances-events";
    public static final String KOGITO_JOBS_EVENTS = "kogito-jobs-events";

    @Inject
    IndexingService indexingService;

    @Inject
    Event<DataEvent<?>> eventPublisher;

    @Incoming(KOGITO_PROCESSINSTANCES_EVENTS)
    public Uni<Void> onProcessInstanceEvent(Collection<ProcessInstanceDataEvent<?>> events) {
        LOGGER.debug("Process instance consumer received ProcessInstanceDataEvents: \n{}", events);
        for (ProcessInstanceDataEvent<?> event : events) {
            try {
                indexingService.indexProcessInstanceEvent(event);
                eventPublisher.fire(event);
            } catch (Exception ex) {
                LOGGER.error("Error processing process instance event: {}", event, ex);
            }

        }
        return Uni.createFrom().voidItem();
    }

    @Incoming(KOGITO_USERTASKINSTANCES_EVENTS)
    public Uni<Void> onUserTaskInstanceEvent(Collection<UserTaskInstanceDataEvent<?>> events) {
        LOGGER.debug("Task instance received UserTaskInstanceDataEvent \n{}", events);
        for (UserTaskInstanceDataEvent<?> event : events) {
            try {
                indexingService.indexUserTaskInstanceEvent(event);
                eventPublisher.fire(event);
            } catch (Exception ex) {
                LOGGER.error("Error processing user task instance event: {}", event, ex);
            }

        }
        return Uni.createFrom().voidItem();
    }

    @Incoming(KOGITO_JOBS_EVENTS)
    public Uni<Void> onJobEvent(KogitoJobCloudEvent event) {
        LOGGER.debug("Job received KogitoJobCloudEvent \n{}", event);
        return Uni.createFrom().item(event)
                .onItem().invoke(e -> indexingService.indexJob(e.getData()))
                .onFailure().invoke(t -> LOGGER.error("Error processing job KogitoJobCloudEvent: {}", t.getMessage(), t))
                .onItem().ignore().andContinueWithNull();
    }

    @Incoming(KOGITO_PROCESS_DEFINITIONS_EVENTS)
    public Uni<Void> onProcessDefinitionDataEvent(Collection<ProcessDefinitionDataEvent> events) {
        LOGGER.debug("Process Definition received ProcessDefinitionDataEvent \n{}", events);
        for (ProcessDefinitionDataEvent event : events) {
            try {
                indexingService.indexProcessDefinition(event);
                eventPublisher.fire(event);
            } catch (Exception ex) {
                LOGGER.error("Error processing process definition event: {}", event, ex);
            }

        }
        return Uni.createFrom().voidItem();
    }
}
