/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.service;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import org.opensearch.search.asynchronous.utils.TestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsynchronousSearchStandbyModeTests extends OpenSearchTestCase {

    public void testCreateContextRejectedInStandbyMode() {
        ThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool(getTestName());
            clusterService = createClusterService(threadPool, standbySettings());
            CountingClient client = new CountingClient(threadPool);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(
                client,
                clusterService,
                threadPool
            );
            AsynchronousSearchService service = new AsynchronousSearchService(
                persistenceService,
                new AsynchronousSearchActiveStore(clusterService),
                client,
                clusterService,
                threadPool,
                new InternalAsynchronousSearchStats(),
                new NamedWriteableRegistry(Collections.emptyList())
            );

            OpenSearchStatusException exception = expectThrows(
                OpenSearchStatusException.class,
                () -> service.createAndStoreContext(new SubmitAsynchronousSearchRequest(new SearchRequest()), 0L, () -> null, null)
            );
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, exception.status());
        } finally {
            if (clusterService != null) {
                clusterService.stop();
            }
            ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    public void testPersistenceWritesSkippedInStandbyMode() {
        ThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool(getTestName());
            clusterService = createClusterService(threadPool, standbySettings());
            CountingClient client = new CountingClient(threadPool);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(
                client,
                clusterService,
                threadPool
            );

            AtomicBoolean storeFailed = new AtomicBoolean();
            persistenceService.storeResponse(
                "id",
                new AsynchronousSearchPersistenceModel(1L, 2L, (String) null, null, null),
                ActionListener.wrap(response -> fail("expected standby mode to reject response storage"), e -> storeFailed.set(true))
            );
            assertTrue(storeFailed.get());

            AtomicBoolean cleanupAcknowledged = new AtomicBoolean();
            persistenceService.deleteExpiredResponses(
                ActionListener.wrap(response -> cleanupAcknowledged.set(response.isAcknowledged()), e -> fail(e.getMessage())),
                10L
            );
            assertTrue(cleanupAcknowledged.get());
            assertEquals(0, client.writeCount);
        } finally {
            if (clusterService != null) {
                clusterService.stop();
            }
            ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    public void testKeepAliveUpdateFallsBackToReadInStandbyMode() {
        ThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool(getTestName());
            clusterService = createClusterService(threadPool, standbySettings());
            AsynchronousSearchPersistenceService persistenceService = mock(AsynchronousSearchPersistenceService.class);
            when(persistenceService.isStandbyModeEnabled()).thenReturn(true);
            doAnswer(invocation -> {
                ActionListener<?> listener = invocation.getArgument(2);
                listener.onFailure(new ResourceNotFoundException("id"));
                return null;
            }).when(persistenceService).getResponse(eq("id"), any(), any());
            AsynchronousSearchActiveStore activeStore = new AsynchronousSearchActiveStore(clusterService);
            AsynchronousSearchService service = new AsynchronousSearchService(
                persistenceService,
                activeStore,
                new CountingClient(threadPool),
                clusterService,
                threadPool,
                new InternalAsynchronousSearchStats(),
                new NamedWriteableRegistry(Collections.emptyList())
            );

            service.updateKeepAliveAndGetContext(
                "id",
                TimeValue.timeValueMinutes(1),
                new AsynchronousSearchContextId(UUIDs.base64UUID(), 1L),
                null,
                ActionListener.wrap(context -> fail("expected missing context"), e -> assertTrue(e instanceof ResourceNotFoundException))
            );

            verify(persistenceService, never()).updateExpirationTime(eq("id"), anyLong(), any(), any());
            verify(persistenceService).getResponse(eq("id"), any(), any());
        } finally {
            if (clusterService != null) {
                clusterService.stop();
            }
            ThreadPool.terminate(threadPool, 30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static Settings standbySettings() {
        return Settings.builder()
            .put("node.name", "test")
            .put("cluster.name", "standby-tests")
            .put(AsynchronousSearchPlugin.CLUSTER_STANDBY_MODE_SETTING.getKey(), true)
            .put(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), 10)
            .build();
    }

    private static ClusterService createClusterService(ThreadPool threadPool, Settings settings) {
        Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                AsynchronousSearchPlugin.CLUSTER_STANDBY_MODE_SETTING,
                AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING
            )
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            org.opensearch.Version.CURRENT
        );
        return TestUtils.createClusterService(settings, threadPool, discoveryNode, clusterSettings);
    }

    private static class CountingClient extends NoOpClient {
        private int writeCount;

        CountingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action instanceof IndexAction || action instanceof DeleteAction || action instanceof DeleteByQueryAction) {
                writeCount++;
            }
            listener.onResponse(null);
        }
    }
}
