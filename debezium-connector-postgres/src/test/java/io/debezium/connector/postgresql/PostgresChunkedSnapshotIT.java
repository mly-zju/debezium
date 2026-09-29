/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * PostgreSQL-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class PostgresChunkedSnapshotIT extends AbstractChunkedSnapshotTest<PostgresConnector> {

    private PostgresConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.createPublicationForAllTables();
        initializeConnectorTestFramework();

        connection = TestHelper.create();

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.afterEach();
    }

    @Test
    @FixFor("dbz#2173")
    public void shouldSnapshotChunkedTableWhoseNameRequiresQuoting() throws Exception {
        final int ROW_COUNT = 1_000;

        // A primary-keyed table whose fully-qualified name requires quoting. Unquoted, the chunked
        // snapshot row-count query would be `SELECT COUNT(1) FROM public.table_with_pk.1#2/3`, which
        // Postgres rejects with "syntax error at or near .1".
        final String qualifiedTableName = "public.\"table_with_pk.1#2/3\"";

        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar(50))".formatted(qualifiedTableName));
        try (PreparedStatement st = connection.connection().prepareStatement("INSERT INTO " + qualifiedTableName + " VALUES (?,?)")) {
            for (int i = 0; i < ROW_COUNT; i++) {
                st.setInt(1, i);
                st.setString(2, String.valueOf(i));
                st.addBatch();
            }
            st.executeBatch();
        }
        connection.commit();

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER, 2)
                .with(RelationalDatabaseConnectorConfig.SCHEMA_INCLUDE_LIST, "public")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(ROW_COUNT);
        assertThat(allRecords.allRecordsInOrder()).hasSize(ROW_COUNT);

        // Confirm the chunked (not legacy) algorithm actually ran, i.e. the previously-failing path.
        assertCreatedChunkSnapshotWorker(2);
    }

    @Test
    @FixFor("dbz#2730")
    public void shouldPlanChunksFromRowCountEstimate() throws Exception {
        final int ROW_COUNT = 1_000;

        createSingleKeyTable("public.dbz2730a");
        populateSingleKeyTable("public.dbz2730a", ROW_COUNT);

        // Make the statistics claim 2 rows, so that planning from the estimate yields 2 chunks where the exact count
        // would yield 4 (2 threads x multiplier 2). VACUUM first so autovacuum doesn't refresh reltuples meanwhile.
        connection.setAutoCommit(true);
        connection.execute("VACUUM ANALYZE public.dbz2730a");
        connection.execute("UPDATE pg_class SET reltuples = 2 WHERE oid = 'public.dbz2730a'::regclass");
        connection.setAutoCommit(false);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER, 2)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "public.dbz2730a")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT + 1)
                .build();

        start(getConnectorClass(), config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted();

        // The underestimate only makes the chunks uneven; every row is still read exactly once
        final List<SourceRecord> records = consumeRecordsByTopic(ROW_COUNT).recordsForTopic(getTableTopicName("dbz2730a"));
        assertThat(records).hasSize(ROW_COUNT);
        assertThat(getRecordKeysForSingleKeyTable(records, getSingleKeyTableKeyColumnName())).hasSize(ROW_COUNT);

        assertTableSnapshotChunked("public.dbz2730a", 2, 2);
        assertChunkedSnapshotFinished(1, 2);
    }

    @Test
    @FixFor("dbz#2730")
    public void shouldSnapshotTableAsSingleChunkWhenChunkPlanningFails() throws Exception {
        final int ROW_COUNT = 1_000;

        createSingleKeyTable("public.dbz2730a");
        populateSingleKeyTable("public.dbz2730a", ROW_COUNT);
        createSingleKeyTable("public.dbz2730b");
        populateSingleKeyTable("public.dbz2730b", ROW_COUNT);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "public.dbz2730a,public.dbz2730b")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2 * ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 2 * ROW_COUNT + 1)
                // The row count of dbz2730a waits for the lock below, and fails once it times out
                .with(CommonConnectorConfig.DRIVER_CONFIG_PREFIX + "options", "-c lock_timeout=3000")
                .build();

        try (PostgresConnection locker = TestHelper.create()) {
            locker.setAutoCommit(false);
            locker.executeWithoutCommitting("LOCK TABLE public.dbz2730a IN ACCESS EXCLUSIVE MODE");

            start(getConnectorClass(), config);
            assertConnectorIsRunning();

            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                    .until(() -> logInterceptor.containsWarnMessage("Table 'public.dbz2730a' chunk planning failed, using single chunk"));
            // Let the data read of dbz2730a through
            locker.rollback();
        }

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(2 * ROW_COUNT);
        assertThat(allRecords.recordsForTopic(getTableTopicName("dbz2730a"))).hasSize(ROW_COUNT);
        assertThat(allRecords.recordsForTopic(getTableTopicName("dbz2730b"))).hasSize(ROW_COUNT);

        // The failed statement aborted no more than its savepoint, so the snapshot transaction could still plan
        // dbz2730b and read both tables
        assertTableSnapshotChunked("public.dbz2730b", 1, 2);
        assertChunkedSnapshotFinished(2, 3);
    }

    @Test
    @FixFor("dbz#2730")
    public void shouldReconnectWhenChunkPlanningLosesMainConnection() throws Exception {
        final int ROW_COUNT = 1_000;

        createSingleKeyTable("public.dbz2730a");
        populateSingleKeyTable("public.dbz2730a", ROW_COUNT);
        createSingleKeyTable("public.dbz2730b");
        populateSingleKeyTable("public.dbz2730b", ROW_COUNT);

        final Configuration config = getConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, "public.dbz2730a,public.dbz2730b")
                .with(CommonConnectorConfig.MAX_BATCH_SIZE, 2 * ROW_COUNT)
                .with(CommonConnectorConfig.MAX_QUEUE_SIZE, 2 * ROW_COUNT + 1)
                .build();

        try (PostgresConnection locker = TestHelper.create()) {
            locker.setAutoCommit(false);
            locker.executeWithoutCommitting("LOCK TABLE public.dbz2730a IN ACCESS EXCLUSIVE MODE");

            start(getConnectorClass(), config);
            assertConnectorIsRunning();

            // Kill the main connection while its row count of dbz2730a waits for the lock, as a socket timeout would
            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                    .until(() -> terminateBackendsWaitingOnLock("COUNT(1)"));
            Awaitility.await().atMost(TestHelper.waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                    .until(() -> logInterceptor.containsWarnMessage("Snapshot main connection is no longer valid after a chunk planning failure"));
            locker.rollback();
        }

        waitForSnapshotToBeCompleted();

        final SourceRecords allRecords = consumeRecordsByTopic(2 * ROW_COUNT);
        assertThat(allRecords.recordsForTopic(getTableTopicName("dbz2730a"))).hasSize(ROW_COUNT);
        assertThat(allRecords.recordsForTopic(getTableTopicName("dbz2730b"))).hasSize(ROW_COUNT);

        assertThat(logInterceptor.containsWarnMessage("Table 'public.dbz2730a' chunk planning failed, using single chunk")).isTrue();
        assertTableSnapshotChunked("public.dbz2730b", 1, 2);
        assertChunkedSnapshotFinished(2, 3);
    }

    private boolean terminateBackendsWaitingOnLock(String queryFragment) throws SQLException {
        try (PostgresConnection admin = TestHelper.create()) {
            return admin.queryAndMap(
                    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%"
                            + queryFragment + "%' AND pid <> pg_backend_pid()",
                    rs -> rs.next() && rs.getLong(1) > 0);
        }
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
    }

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig();
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "public.dbz1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("public.dbz1220a", "public.dbz1220b", "public.dbz1220c", "public.dbz1220d"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar(50))".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), org_name varchar(50), data varchar(50), primary key(id, org_name))".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), data varchar(50))".formatted(tableName));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "id";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("id", "org_name");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return "test_server.%s.%s".formatted("public", tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "public.%s".formatted(tableName);
    }

}
