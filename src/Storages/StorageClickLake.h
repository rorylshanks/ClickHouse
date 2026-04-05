#pragma once

#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPHeaderEntries.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/logger_useful.h>

namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class ActionsDAG;

/**
 * ClickLake table engine - connects to a remote ClickLake server for query planning.
 *
 * For SELECT queries:
 *   1. Sends query AST/info to the ClickLake server via HTTP POST
 *   2. Receives a list of S3/HTTP URLs containing the data
 *   3. Reads data from those URLs using standard ClickHouse format readers
 *
 * For INSERT queries:
 *   1. Streams data to the ClickLake server in Arrow format via HTTP POST
 *
 * For ALTER/DELETE:
 *   1. Forwards the mutation to the ClickLake server asynchronously
 *   2. Returns immediately (mutations handled async by server)
 *
 * Usage: ENGINE = ClickLake('http://server:port/tablename')
 */
class StorageClickLake final : public IStorage
{
public:
    StorageClickLake(
        const String & server_url_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        ContextPtr context_);

    String getName() const override { return "ClickLake"; }

    /// SELECT - sends query to server, gets URLs, reads from them
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// INSERT - streams data to server in Arrow format
    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    /// ALTER support
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;

    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const { return true; }
    bool supportsParallelInsert() const override { return true; }
    bool prefersLargeBlocks() const override { return true; }
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    std::optional<NameSet> supportedPrewhereColumns() const override;
    ColumnSizeByName getColumnSizes() const override;

    /// Response from ClickLake server for SELECT queries
    struct QueryFile
    {
        String url;
        String etag;
        UInt64 size = 0;
        UInt64 last_modified = 0; /// Unix timestamp (seconds)
    };

    struct QueryResponse
    {
        std::vector<QueryFile> files;       /// S3/HTTP file objects to read from
        std::optional<String> continuation_token;  /// For paginated results
        std::optional<String> query_id;     /// Snapshot query ID
        String format;                      /// Data format (e.g., "Parquet")
        std::optional<String> error;        /// Error message if any
    };

    /// Send SELECT query info to server and get URLs to read from
    /// If cached_request_body is provided (non-null), it will be populated with the
    /// JSON request body for reuse in pagination requests.
    QueryResponse sendQueryToServer(
        const String & query_text,
        const SelectQueryInfo & query_info,
        const Names & required_columns,
        const std::shared_ptr<const ActionsDAG> & filter_dag,
        std::optional<size_t> limit,
        const std::optional<String> & continuation_token,
        ContextPtr context,
        String * cached_request_body = nullptr) const;

    /// Send a paginated query using a cached request body from the initial request.
    /// This preserves all optimization hints (filter_dag, where_ast, etc.) across pages.
    QueryResponse sendPaginatedQuery(
        const String & cached_request_body,
        const String & continuation_token,
        const std::optional<String> & query_id,
        ContextPtr context) const;

private:
    String server_url;
    LoggerPtr log;

    /// Send JSON request body to server and return response (shared by sendQueryToServer and sendPaginatedQuery)
    QueryResponse sendJsonRequest(const String & body_str, ContextPtr context) const;

    /// Send mutation (ALTER/DELETE) to server asynchronously
    void sendMutationToServer(const String & mutation_text, ContextPtr context) const;

    /// Parse JSON response from server
    QueryResponse parseQueryResponse(const String & response_body) const;

    /// Get HTTP timeouts from context
    ConnectionTimeouts getTimeouts(ContextPtr context) const;
};


/// Sink for INSERT - streams data to ClickLake server in Arrow format
class ClickLakeSink : public SinkToStorage
{
public:
    ClickLakeSink(
        const String & server_url,
        const Block & sample_block,
        ContextPtr context,
        const ConnectionTimeouts & timeouts);

    ~ClickLakeSink() override;

    String getName() const override { return "ClickLakeSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void initBuffers();
    void finalizeBuffers();

    String insert_url;
    ContextPtr context;
    ConnectionTimeouts timeouts;

    std::unique_ptr<WriteBuffer> http_buffer;
    OutputFormatPtr arrow_writer;
    bool initialized = false;
};

void registerStorageClickLake(StorageFactory & factory);

}
