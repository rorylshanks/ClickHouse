#include <Storages/StorageClickLake.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/AlterCommands.h>
#include <Storages/prepareReadingFromFormat.h>

#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Core/Settings.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <IO/S3Settings.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3/URI.h>
#endif

#include <Poco/URI.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/NetException.h>
#include <Poco/StreamCopier.h>

#include <boost/algorithm/string.hpp>
#include <Poco/Timespan.h>

#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
#if USE_AWS_S3
    extern const SettingsBool allow_archive_path_syntax;
    extern const SettingsBool compatibility_s3_presigned_url_query_in_path;
    extern const SettingsBool s3_validate_request_settings;
#endif
}

namespace
{
String getASTNodeType(const IAST & ast)
{
    String id = ast.getID(' ');
    auto pos = id.find(' ');
    if (pos == String::npos)
        return id;
    return id.substr(0, pos);
}

Poco::JSON::Object::Ptr astToJSON(const ASTPtr & ast)
{
    if (!ast)
        return nullptr;

    auto node = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    node->set("node_type", getASTNodeType(*ast));
    node->set("id", ast->getID(' '));
    bool children_serialized = false;

    if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(ast.get()))
    {
        if (!with_alias->alias.empty())
            node->set("alias", with_alias->alias);
    }

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        node->set("name", identifier->name());
        node->set("short_name", identifier->shortName());
        node->set("is_compound", identifier->compound());
        Poco::JSON::Array name_parts;
        for (const auto & part : identifier->name_parts)
            name_parts.add(part);
        node->set("name_parts", name_parts);
    }
    else if (const auto * literal = ast->as<ASTLiteral>())
    {
        node->set("value_type", String(literal->value.getTypeName()));
        node->set("value_text", applyVisitor(FieldVisitorToString(), literal->value));
    }
    else if (const auto * function = ast->as<ASTFunction>())
    {
        node->set("name", function->name);
        node->set("is_operator", function->isOperator());
        node->set("is_window_function", function->isWindowFunction());
        node->set("is_lambda_function", function->isLambdaFunction());

        if (function->arguments)
        {
            Poco::JSON::Array arguments;
            for (const auto & arg : function->arguments->children)
                arguments.add(astToJSON(arg));
            node->set("arguments", arguments);
        }

        if (function->parameters)
        {
            Poco::JSON::Array parameters;
            for (const auto & param : function->parameters->children)
                parameters.add(astToJSON(param));
            node->set("parameters", parameters);
        }

        /// ASTFunction keeps arguments/parameters in children as well.
        /// Serializing both specific fields and generic children duplicates subtrees.
        children_serialized = true;
    }
    else if (const auto * expression_list = ast->as<ASTExpressionList>())
    {
        Poco::JSON::Array items;
        for (const auto & child : expression_list->children)
            items.add(astToJSON(child));
        node->set("items", items);

        /// ASTExpressionList::children and "items" represent the same list.
        children_serialized = true;
    }

    if (!children_serialized && !ast->children.empty())
    {
        Poco::JSON::Array children;
        for (const auto & child : ast->children)
            children.add(astToJSON(child));
        node->set("children", children);
    }

    return node;
}

const ASTSelectQuery * tryGetSelectQuery(const ASTPtr & query)
{
    if (!query)
        return nullptr;

    if (const auto * select = query->as<ASTSelectQuery>())
        return select;

    if (const auto * select_union = query->as<ASTSelectWithUnionQuery>())
    {
        if (select_union->list_of_selects && !select_union->list_of_selects->children.empty())
            return select_union->list_of_selects->children.front()->as<ASTSelectQuery>();
    }

    return nullptr;
}
}

#if USE_AWS_S3
namespace
{
struct ClickLakeS3GroupKey
{
    String endpoint;
    String bucket;

    bool operator<(const ClickLakeS3GroupKey & other) const
    {
        if (endpoint != other.endpoint)
            return endpoint < other.endpoint;
        return bucket < other.bucket;
    }
};

struct ClickLakeS3GroupInfo
{
    S3::URI base_uri;
    size_t initial_size = 0;
    StorageObjectStorageConfigurationPtr configuration;
    ObjectStoragePtr object_storage;
    size_t streams = 0;
};

class ClickLakeS3Configuration final : public StorageS3Configuration
{
public:
    void markInitialized()
    {
        initialized = true;
        setPathForRead(getRawPath());
    }
};

std::optional<S3::URI> tryParseS3URI(const String & url, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    return S3::URI(
        url,
        settings[Setting::allow_archive_path_syntax],
        /*keep_presigned_query_parameters*/ !settings[Setting::compatibility_s3_presigned_url_query_in_path]);
}
}
#endif


StorageClickLake::StorageClickLake(
    const String & server_url_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_,
    ContextPtr /*context_*/)
    : IStorage(table_id_)
    , server_url(server_url_)
    , log(getLogger("StorageClickLake"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);

    LOG_INFO(log, "Created ClickLake storage with server URL: {}", server_url);
}

bool StorageClickLake::supportsPrewhere() const
{
    return true;
}

bool StorageClickLake::canMoveConditionsToPrewhere() const
{
    return true;
}

std::optional<NameSet> StorageClickLake::supportedPrewhereColumns() const
{
    return getInMemoryMetadataPtr()->getColumnsWithoutDefaultExpressions(/*exclude=*/ {});
}

IStorage::ColumnSizeByName StorageClickLake::getColumnSizes() const
{
    return getInMemoryMetadataPtr()->getFakeColumnSizes();
}


ConnectionTimeouts StorageClickLake::getTimeouts(ContextPtr local_context) const
{
    return ConnectionTimeouts::getHTTPTimeouts(local_context->getSettingsRef(), local_context->getServerSettings());
}


StorageClickLake::QueryResponse StorageClickLake::parseQueryResponse(const String & response_body) const
{
    QueryResponse response;

    try
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(response_body);
        auto root = json.extract<Poco::JSON::Object::Ptr>();

        if (root->has("error"))
        {
            response.error = root->getValue<String>("error");
            return response;
        }

        if (root->has("files"))
        {
            auto files_array = root->getArray("files");
            const auto files_size = static_cast<unsigned int>(files_array->size());
            for (unsigned int i = 0; i < files_size; ++i)
            {
                QueryFile file;

                /// Support both object entries and plain string entries for compatibility.
                if (files_array->isObject(i))
                {
                    auto file_obj = files_array->getObject(i);
                    if (file_obj->has("url"))
                        file.url = file_obj->getValue<String>("url");
                    if (file_obj->has("etag"))
                        file.etag = file_obj->getValue<String>("etag");
                    if (file_obj->has("size"))
                        file.size = file_obj->getValue<UInt64>("size");
                    if (file_obj->has("last_modified"))
                        file.last_modified = file_obj->getValue<UInt64>("last_modified");
                }
                else
                {
                    try
                    {
                        file.url = files_array->getElement<String>(i);
                    }
                    catch (const Poco::Exception &)
                    {
                        throw Exception(
                            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                            "Invalid `files` entry at index {}: expected object or string", i);
                    }
                }

                response.files.push_back(std::move(file));
            }
        }
        else if (root->has("urls"))
        {
            /// Backward compatibility with older ClickLake server responses.
            auto urls_array = root->getArray("urls");
            const auto urls_size = static_cast<unsigned int>(urls_array->size());
            for (unsigned int i = 0; i < urls_size; ++i)
            {
                QueryFile file;
                file.url = urls_array->getElement<String>(i);
                response.files.push_back(std::move(file));
            }
        }

        if (root->has("format"))
            response.format = root->getValue<String>("format");
        else
            response.format = "Parquet";  /// Default format

        if (root->has("continuation_token"))
            response.continuation_token = root->getValue<String>("continuation_token");

        if (root->has("query_id"))
            response.query_id = root->getValue<String>("query_id");
    }
    catch (const Poco::Exception & e)
    {
        response.error = "Failed to parse server response: " + e.displayText();
    }

    return response;
}


StorageClickLake::QueryResponse StorageClickLake::sendQueryToServer(
    const String & query_text,
    const SelectQueryInfo & query_info,
    const Names & required_columns,
    const std::shared_ptr<const ActionsDAG> & filter_dag,
    std::optional<size_t> limit,
    const std::optional<String> & continuation_token,
    ContextPtr local_context,
    String * cached_request_body) const
{
    Poco::URI query_uri(server_url);
    query_uri.setPath(query_uri.getPath() + "/query");

    LOG_DEBUG(log, "Sending query to ClickLake server: {}", query_uri.toString());

    auto timeouts = getTimeouts(local_context);

    /// Build JSON request body with optimization info (done once, reused for retries)
    Poco::JSON::Object json_request;
    json_request.set("query", query_text);
    json_request.set("table", getStorageID().getTableName());
    json_request.set("database", getStorageID().getDatabaseName());
    if (query_info.table_expression && query_info.table_expression->hasAlias())
        json_request.set("table_alias", query_info.table_expression->getAlias());
    if (continuation_token)
        json_request.set("continuation_token", *continuation_token);

    /// Add required columns (column projection)
    Poco::JSON::Array columns_array;
    for (const auto & col : required_columns)
        columns_array.add(col);
    json_request.set("required_columns", columns_array);

    /// Add schema information
    Poco::JSON::Array schema_array;
    const auto & columns = getInMemoryMetadata().getColumns();
    for (const auto & col : columns.getAll())
    {
        Poco::JSON::Object col_obj;
        col_obj.set("name", col.name);
        col_obj.set("type", col.type->getName());
        schema_array.add(col_obj);
    }
    json_request.set("schema", schema_array);

    /// Add optimization hints
    Poco::JSON::Object optimization;

    /// Limit pushdown
    if (limit)
        optimization.set("limit", static_cast<UInt64>(*limit));
    if (query_info.trivial_limit > 0)
        optimization.set("trivial_limit", query_info.trivial_limit);

    /// Query characteristics
    optimization.set("has_aggregates", query_info.has_aggregates);
    optimization.set("has_order_by", query_info.has_order_by);
    optimization.set("has_window", query_info.has_window);
    optimization.set("need_aggregate", query_info.need_aggregate);

    /// Filter/predicate pushdown info
    auto filter_info = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    if (filter_dag)
    {
        filter_info->set("filter_dag", filter_dag->dumpDAG());
        filter_info->set("filter_columns", filter_dag->dumpNames());

        /// Extract filter output nodes (the actual predicates)
        Poco::JSON::Array filter_outputs;
        for (const auto * node : filter_dag->getOutputs())
        {
            Poco::JSON::Object node_info;
            node_info.set("name", node->result_name);
            node_info.set("type", node->result_type->getName());
            filter_outputs.add(node_info);
        }
        filter_info->set("output_columns", filter_outputs);
    }

    if (const auto * select_query = tryGetSelectQuery(query_info.query))
    {
        if (auto where_ast = select_query->where())
            filter_info->set("where_ast", astToJSON(where_ast));
        if (auto prewhere_ast = select_query->prewhere())
            filter_info->set("prewhere_ast", astToJSON(prewhere_ast));
    }

    if (!query_info.filter_asts.empty())
    {
        Poco::JSON::Array extra_filters;
        for (const auto & filter_ast : query_info.filter_asts)
        {
            if (!filter_ast)
                continue;
            Poco::JSON::Object entry;
            entry.set("ast", astToJSON(filter_ast));
            extra_filters.add(entry);
        }
        if (extra_filters.size() > 0)
            filter_info->set("extra_filter_asts", extra_filters);
    }

    if (query_info.row_level_filter)
    {
        Poco::JSON::Object row_level_info;
        row_level_info.set("dag", query_info.row_level_filter->actions.dumpDAG());
        row_level_info.set("column", query_info.row_level_filter->column_name);
        row_level_info.set("remove_column", query_info.row_level_filter->do_remove_column);
        filter_info->set("row_level_actions", row_level_info);
    }

    if (query_info.prewhere_info)
    {
        Poco::JSON::Object prewhere_info;
        prewhere_info.set("dag", query_info.prewhere_info->prewhere_actions.dumpDAG());
        prewhere_info.set("column", query_info.prewhere_info->prewhere_column_name);
        prewhere_info.set("need_filter", query_info.prewhere_info->need_filter);
        prewhere_info.set("remove_column", query_info.prewhere_info->remove_prewhere_column);
        filter_info->set("prewhere_actions", prewhere_info);
    }

    optimization.set("filter", filter_info);

    /// Input order info (for sorted reads)
    if (query_info.input_order_info)
    {
        Poco::JSON::Object order_info;
        order_info.set("direction", query_info.input_order_info->direction);
        order_info.set("limit", query_info.input_order_info->limit);

        Poco::JSON::Array sort_desc;
        for (const auto & col : query_info.input_order_info->sort_description_for_merging)
        {
            Poco::JSON::Object sort_col;
            sort_col.set("column", col.column_name);
            sort_col.set("direction", col.direction);
            sort_col.set("nulls_direction", col.nulls_direction);
            sort_desc.add(sort_col);
        }
        order_info.set("sort_description", sort_desc);

        optimization.set("input_order", order_info);
    }

    json_request.set("optimization", optimization);

    std::ostringstream request_body_stream;
    json_request.stringify(request_body_stream);
    String body_str = request_body_stream.str();

    /// Cache the request body (without continuation_token) for pagination reuse
    if (cached_request_body)
    {
        /// Remove continuation_token from cached body so pagination can add its own
        Poco::JSON::Object json_for_cache;
        json_for_cache = json_request;  // Copy
        json_for_cache.remove("continuation_token");
        std::ostringstream cache_stream;
        json_for_cache.stringify(cache_stream);
        *cached_request_body = cache_stream.str();
    }

    return sendJsonRequest(body_str, local_context);
}


StorageClickLake::QueryResponse StorageClickLake::sendPaginatedQuery(
    const String & cached_request_body,
    const String & continuation_token,
    const std::optional<String> & query_id,
    ContextPtr local_context) const
{
    /// Parse the cached request body and add the continuation token
    Poco::JSON::Parser parser;
    auto json = parser.parse(cached_request_body);
    auto json_request = json.extract<Poco::JSON::Object::Ptr>();
    json_request->set("continuation_token", continuation_token);
    if (query_id)
        json_request->set("query_id", *query_id);

    std::ostringstream request_body_stream;
    json_request->stringify(request_body_stream);

    return sendJsonRequest(request_body_stream.str(), local_context);
}


StorageClickLake::QueryResponse StorageClickLake::sendJsonRequest(
    const String & body_str,
    ContextPtr local_context) const
{
    Poco::URI query_uri(server_url);
    query_uri.setPath(query_uri.getPath() + "/query");

    LOG_DEBUG(log, "Sending request to ClickLake server: {}, body: {}", query_uri.toString(), body_str);

    auto timeouts = getTimeouts(local_context);

    /// Retry configuration
    constexpr size_t max_retries = 3;
    constexpr size_t initial_backoff_ms = 100;
    constexpr size_t max_backoff_ms = 5000;

    String last_error;
    for (size_t attempt = 0; attempt <= max_retries; ++attempt)
    {
        if (attempt > 0)
        {
            size_t backoff_ms = std::min(initial_backoff_ms * (1 << (attempt - 1)), max_backoff_ms);
            LOG_WARNING(log, "Retry attempt {} after {}ms (last error: {})", attempt, backoff_ms, last_error);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }

        try
        {
            Poco::Net::HTTPClientSession session(query_uri.getHost(), query_uri.getPort());
            session.setTimeout(
                timeouts.connection_timeout,
                timeouts.send_timeout,
                timeouts.receive_timeout);

            Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST,
                                           query_uri.getPathAndQuery(),
                                           Poco::Net::HTTPMessage::HTTP_1_1);
            http_request.setContentType("application/json");
            http_request.setContentLength(body_str.size());

            std::ostream & request_stream = session.sendRequest(http_request);
            request_stream << body_str;
            request_stream.flush();

            Poco::Net::HTTPResponse response;
            std::istream & response_stream = session.receiveResponse(response);

            String response_body;
            Poco::StreamCopier::copyToString(response_stream, response_body);

            auto status = response.getStatus();

            if (status >= 500 || status == 429)
            {
                last_error = fmt::format("HTTP error {}: {}", status, response_body);
                if (attempt < max_retries)
                    continue;
                QueryResponse error_response;
                error_response.error = last_error;
                return error_response;
            }

            if (status != Poco::Net::HTTPResponse::HTTP_OK)
            {
                QueryResponse error_response;
                error_response.error = fmt::format("HTTP error {}: {}", status, response_body);
                return error_response;
            }

            LOG_DEBUG(log, "Response from ClickLake server: {}", response_body);

            return parseQueryResponse(response_body);
        }
        catch (const Poco::Net::ConnectionRefusedException & e)
        {
            last_error = "Connection refused: " + e.displayText();
            if (attempt < max_retries)
                continue;
        }
        catch (const Poco::TimeoutException & e)
        {
            last_error = "Timeout: " + e.displayText();
            if (attempt < max_retries)
                continue;
        }
        catch (const Poco::Net::NetException & e)
        {
            last_error = "Network error: " + e.displayText();
            if (attempt < max_retries)
                continue;
        }
        catch (const Exception & e)
        {
            QueryResponse error_response;
            error_response.error = "Failed to communicate with ClickLake server: " + e.message();
            return error_response;
        }
        catch (const Poco::Exception & e)
        {
            last_error = e.displayText();
            if (attempt < max_retries)
                continue;
        }
    }

    QueryResponse error_response;
    error_response.error = "Failed to communicate with ClickLake server after " + std::to_string(max_retries + 1) + " attempts: " + last_error;
    return error_response;
}


void StorageClickLake::sendMutationToServer(const String & mutation_text, ContextPtr local_context) const
{
    Poco::URI mutate_uri(server_url);
    mutate_uri.setPath(mutate_uri.getPath() + "/mutate");

    LOG_DEBUG(log, "Sending mutation to ClickLake server: {}", mutate_uri.toString());

    try
    {
        auto timeouts = getTimeouts(local_context);

        Poco::JSON::Object json_request;
        json_request.set("mutation", mutation_text);
        json_request.set("table", getStorageID().getTableName());
        json_request.set("database", getStorageID().getDatabaseName());

        std::ostringstream request_body_stream;
        json_request.stringify(request_body_stream);
        String body_str = request_body_stream.str();

        /// Create HTTP request using Poco directly
        Poco::Net::HTTPClientSession session(mutate_uri.getHost(), mutate_uri.getPort());
        session.setTimeout(
            timeouts.connection_timeout,
            timeouts.send_timeout,
            timeouts.receive_timeout);

        Poco::Net::HTTPRequest http_request(Poco::Net::HTTPRequest::HTTP_POST,
                                           mutate_uri.getPathAndQuery(),
                                           Poco::Net::HTTPMessage::HTTP_1_1);
        http_request.setContentType("application/json");
        http_request.setContentLength(body_str.size());

        /// Send request with body
        std::ostream & request_stream = session.sendRequest(http_request);
        request_stream << body_str;
        request_stream.flush();

        /// Receive response (we don't care about the result for async mutations)
        Poco::Net::HTTPResponse response;
        std::istream & response_stream = session.receiveResponse(response);
        String response_body;
        Poco::StreamCopier::copyToString(response_stream, response_body);

        LOG_DEBUG(log, "Mutation sent successfully, status: {}", response.getStatus());
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Failed to send mutation to ClickLake server: {}", e.message());
        throw;
    }
}


/// Custom read step that fetches URLs from ClickLake server and reads from them
class ReadFromClickLake : public SourceStepWithFilter
{
public:
    ReadFromClickLake(
        const Names & column_names_,
        std::shared_ptr<const StorageClickLake> storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info_,
        ContextPtr context_,
        size_t max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , max_block_size(max_block_size_)
        , log(getLogger("ReadFromClickLake"))
    {
    }

    String getName() const override { return "ReadFromClickLake"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::shared_ptr<const StorageClickLake> storage;
    size_t max_block_size;
    LoggerPtr log;
};

namespace
{

struct URLPaginationState
{
    std::mutex mutex;
    std::condition_variable cv;
#if USE_AWS_S3
    std::map<ClickLakeS3GroupKey, std::deque<ObjectInfo>> pending_s3;
#endif
    bool done = false;
    std::atomic<bool> cancelled{false};
    std::optional<String> error;
    std::thread worker;

    void cancel()
    {
        cancelled.store(true);
        cv.notify_all();
    }

    ~URLPaginationState()
    {
        cancel();
        if (worker.joinable())
            worker.join();
    }
};

#if USE_AWS_S3
class ClickLakeS3Source final : public StorageObjectStorageSource
{
public:
    ClickLakeS3Source(
        String name_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        StorageSnapshotPtr storage_snapshot_,
        const ReadFromFormatInfo & info,
        const std::optional<FormatSettings> & format_settings_,
        ContextPtr context_,
        UInt64 max_block_size_,
        std::shared_ptr<IObjectIterator> file_iterator_,
        FormatParserSharedResourcesPtr parser_shared_resources_,
        FormatFilterInfoPtr format_filter_info_,
        bool need_only_count_,
        std::shared_ptr<URLPaginationState> state_)
        : StorageObjectStorageSource(
            std::move(name_),
            std::move(object_storage_),
            std::move(configuration_),
            std::move(storage_snapshot_),
            info,
            format_settings_,
            std::move(context_),
            max_block_size_,
            std::move(file_iterator_),
            std::move(parser_shared_resources_),
            std::move(format_filter_info_),
            need_only_count_)
        , state(std::move(state_))
    {
    }

    ~ClickLakeS3Source() override
    {
        if (state)
            state->cancel();
    }

    void onFinish() override
    {
        StorageObjectStorageSource::onFinish();
        if (state)
            state->cancel();
    }

protected:
    void onCancel() noexcept override
    {
        if (state)
            state->cancel();
    }

private:
    std::shared_ptr<URLPaginationState> state;
};
#endif

}


void ReadFromClickLake::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// Get query text
    const auto & local_query_info = getQueryInfo();
    String query_text = local_query_info.query ? local_query_info.query->formatForLogging() : "";

    /// Get optimization info
    const auto & column_names = requiredSourceColumns();
    const auto & filter_dag = getFilterActionsDAG();

    /// Send query to ClickLake server with optimization info
    /// Cache the request body for pagination (preserves all optimization hints)
    String cached_request_body;
    auto response = storage->sendQueryToServer(
        query_text,
        local_query_info,
        column_names,
        filter_dag,
        limit,
        std::nullopt,
        getContext(),
        &cached_request_body);

    if (response.error)
    {
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "ClickLake server returned error: {}", *response.error);
    }

    const auto & local_storage_snapshot = getStorageSnapshot();
    const auto & local_context = getContext();
    auto init_empty_pipeline = [&]()
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
    };

    if (response.files.empty())
    {
        if (response.continuation_token)
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "ClickLake returned invalid first page: continuation_token without files");
        }

        /// No data to read - return empty source.
        LOG_DEBUG(log, "No files returned from ClickLake server, returning empty result");
        init_empty_pipeline();
        return;
    }

    LOG_DEBUG(log, "Got {} files from ClickLake server, format: {}", response.files.size(), response.format);

    /// Keep the context alive for the duration of the pipeline execution.
    /// This is critical for correlated subqueries where the subquery's context
    /// might otherwise be destroyed before the sources finish reading data.
    pipeline.addContext(local_context);

#if USE_AWS_S3
    class ClickLakeS3Iterator : public IObjectIterator
    {
    public:
        ClickLakeS3Iterator(
            std::shared_ptr<URLPaginationState> state_,
            ClickLakeS3GroupKey group_key_)
            : state(std::move(state_))
            , group_key(std::move(group_key_))
        {
        }

        ObjectInfoPtr next(size_t) override
        {
            std::unique_lock lock(state->mutex);
            state->cv.wait(lock, [&]
            {
                return state->cancelled.load()
                    || state->error
                    || !state->pending_s3[group_key].empty()
                    || state->done;
            });

            if (state->cancelled.load())
                return {};

            if (state->error)
                throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "ClickLake pagination error: {}", *state->error);

            if (state->pending_s3[group_key].empty())
                return {};

            ObjectInfo obj = std::move(state->pending_s3[group_key].front());
            state->pending_s3[group_key].pop_front();
            return std::make_shared<ObjectInfo>(std::move(obj));
        }

        size_t estimatedKeysCount() override
        {
            std::lock_guard lock(state->mutex);
            auto it = state->pending_s3.find(group_key);
            if (it == state->pending_s3.end())
                return 0;
            size_t count = it->second.size();
            if (!state->done)
                ++count;
            return count;
        }

    private:
        std::shared_ptr<URLPaginationState> state;
        ClickLakeS3GroupKey group_key;
    };
#endif


    /// Create a source for each URL using StorageURL's reading mechanism
    Pipes pipes;

    const auto & settings = local_context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, local_context->getServerSettings());
    auto read_from_format_info = prepareReadingFromFormat(
        column_names,
        local_storage_snapshot,
        local_context,
        /*supports_subset_of_columns*/ true,
        /*supports_tuple_elements*/ boost::iequals(response.format, "Parquet")
            && ::DB::getFormatSettings(local_context).parquet.use_native_reader_v3,
        /*supports_dynamic_subcolumns*/ boost::iequals(response.format, "Parquet")
            && ::DB::getFormatSettings(local_context).parquet.use_native_reader_v3);
    if (local_query_info.prewhere_info || local_query_info.row_level_filter)
        read_from_format_info = updateFormatPrewhereInfo(read_from_format_info, local_query_info.row_level_filter, local_query_info.prewhere_info);

    /// Create shared resources for format parsing (shared across all URL sources)
    size_t max_streams = settings[Setting::max_threads];
    if (max_streams == 0)
        max_streams = 1;

    auto state = std::make_shared<URLPaginationState>();
#if USE_AWS_S3
    auto s3_groups = std::make_shared<std::map<ClickLakeS3GroupKey, ClickLakeS3GroupInfo>>();
    auto add_file = [state, s3_groups, local_context](const StorageClickLake::QueryFile & file, bool allow_new_groups, std::optional<String> & error)
    {
        try
        {
            if (file.url.empty())
            {
                error = "ClickLake file entry has empty url";
                return;
            }

            auto s3_uri = tryParseS3URI(file.url, local_context);
            ClickLakeS3GroupKey key{.endpoint = s3_uri->endpoint, .bucket = s3_uri->bucket};
            auto it = s3_groups->find(key);
            if (it == s3_groups->end())
            {
                if (!allow_new_groups)
                {
                    error = fmt::format("ClickLake returned unexpected S3 endpoint/bucket: {}/{}", key.endpoint, key.bucket);
                    return;
                }
                it = s3_groups->emplace(
                    key,
                    ClickLakeS3GroupInfo{
                        .base_uri = *s3_uri,
                        .initial_size = 0,
                        .configuration = nullptr,
                        .object_storage = nullptr,
                        .streams = 0,
                    }).first;
            }
            if (allow_new_groups)
                it->second.initial_size++;
            std::optional<ObjectMetadata> metadata;
            if (file.size > 0 || !file.etag.empty() || file.last_modified > 0)
            {
                ObjectMetadata object_metadata;
                object_metadata.size_bytes = file.size;
                object_metadata.etag = file.etag;
                if (file.last_modified > 0)
                {
                    object_metadata.last_modified.fromEpochTime(static_cast<std::time_t>(file.last_modified));
                }
                metadata = std::move(object_metadata);
            }

            /// Key/version are sourced from URL only.
            RelativePathWithMetadata path_with_metadata{s3_uri->key, metadata, s3_uri->version_id};
            state->pending_s3[key].emplace_back(path_with_metadata, s3_uri->version_id);
            return;
        }
        catch (const Exception & e)
        {
            error = e.message();
            return;
        }
        catch (const Poco::Exception & e)
        {
            error = e.displayText();
            return;
        }
    };
#else
    auto add_file = [](const StorageClickLake::QueryFile &, bool, std::optional<String> & error)
    {
        error = "ClickHouse was built without AWS S3 support";
    };
#endif

    {
        std::lock_guard lock(state->mutex);
        for (const auto & file : response.files)
            add_file(file, /*allow_new_groups*/ true, state->error);
        if (state->error)
        {
            state->done = true;
        }
        else if (!response.continuation_token)
        {
            state->done = true;
        }
    }
    if (state->error)
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "ClickLake returned invalid files: {}", *state->error);

    if (response.continuation_token)
    {
        /// Use the cached request body for pagination - this preserves ALL optimization hints
        /// (filter_dag, where_ast, prewhere_ast, etc.) across all paginated requests.
        auto continuation_token = response.continuation_token;
        auto query_id = response.query_id;
        auto storage_ptr = storage;

        state->worker = std::thread([state, storage_ptr, cached_request_body, local_context, continuation_token, query_id, add_file]() mutable
        {
            auto token = continuation_token;
            while (token && !state->cancelled.load())
            {
                /// Send paginated request using cached body (preserves all optimization hints)
                auto next_response = storage_ptr->sendPaginatedQuery(
                    cached_request_body,
                    *token,
                    query_id,
                    local_context);

                if (state->cancelled.load())
                    return;

                if (next_response.error)
                {
                    std::lock_guard lock(state->mutex);
                    state->error = *next_response.error;
                    state->done = true;
                    state->cv.notify_all();
                    return;
                }

                {
                    std::lock_guard lock(state->mutex);
                    for (const auto & file : next_response.files)
                        add_file(file, /*allow_new_groups*/ false, state->error);
                    if (state->error)
                        state->done = true;
                }
                state->cv.notify_all();
                if (state->error)
                    return;
                token = next_response.continuation_token;
            }

            std::lock_guard lock(state->mutex);
            state->done = true;
            state->cv.notify_all();
        });
    }

#if USE_AWS_S3
    if (!s3_groups->empty())
    {
        size_t total_s3_files = 0;
        for (const auto & [key, group] : *s3_groups)
            total_s3_files += group.initial_size;

        size_t total_s3_streams = response.continuation_token
            ? max_streams
            : (total_s3_files > 0 ? std::min(max_streams, total_s3_files) : 1);
        if (total_s3_streams == 0)
            total_s3_streams = 1;

        size_t remaining_streams = total_s3_streams;
        size_t remaining_files = total_s3_files;
        for (auto & [key, group] : *s3_groups)
        {
            if (remaining_streams == 0)
                break;
            size_t group_streams = remaining_files > 0
                ? std::max<size_t>(1, total_s3_streams * group.initial_size / total_s3_files)
                : 1;
            group_streams = std::min(group_streams, remaining_streams);
            group.streams = group_streams;
            remaining_streams -= group_streams;
            if (remaining_files >= group.initial_size)
                remaining_files -= group.initial_size;
        }

        while (remaining_streams > 0)
        {
            for (auto & [key, group] : *s3_groups)
            {
                if (remaining_streams == 0)
                    break;
                ++group.streams;
                --remaining_streams;
            }
        }

        auto s3_parser_shared_resources = std::make_shared<FormatParserSharedResources>(settings, total_s3_streams);
        auto s3_format_filter_info = std::make_shared<FormatFilterInfo>(
            filter_dag,
            local_context,
            nullptr,
            local_query_info.row_level_filter,
            local_query_info.prewhere_info);

        const auto & config = local_context->getConfigRef();
        for (auto & [key, group] : *s3_groups)
        {
            auto configuration = std::make_shared<ClickLakeS3Configuration>();
            configuration->url = group.base_uri;
            configuration->keys = {StorageObjectStorageConfiguration::Path{group.base_uri.key}};
            configuration->format = response.format;
            configuration->compression_method = "auto";
            configuration->structure = "auto";
            configuration->partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;

            configuration->s3_settings = std::make_unique<S3Settings>();
            configuration->s3_settings->loadFromConfigForObjectStorage(
                config,
                "s3",
                settings,
                group.base_uri.uri.getScheme(),
                settings[Setting::s3_validate_request_settings]);

            if (auto endpoint_settings = local_context->getStorageS3Settings().getSettings(group.base_uri.uri.toString(), local_context->getUserName()))
            {
                configuration->s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
                configuration->s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
            }

            configuration->s3_capabilities = std::make_unique<S3Capabilities>(getCapabilitiesFromConfig(config, "s3"));
            configuration->static_configuration = true;
            configuration->markInitialized();

            group.configuration = configuration;
            group.object_storage = configuration->createObjectStorage(local_context, /*is_readonly*/ false, std::nullopt);

            auto iterator = std::make_shared<ClickLakeS3Iterator>(state, key);

            for (size_t stream = 0; stream < group.streams; ++stream)
            {
                auto source = std::make_shared<ClickLakeS3Source>(
                    String("ClickLakeS3Source"),
                    group.object_storage,
                    configuration,
                    local_storage_snapshot,
                    read_from_format_info,
                    /*format_settings*/ std::nullopt,
                    local_context,
                    max_block_size,
                    iterator,
                    s3_parser_shared_resources,
                    s3_format_filter_info,
                    /*need_only_count*/ false,
                    state);

                pipes.emplace_back(std::move(source));
            }
        }
    }
#endif

    if (pipes.empty())
        throw Exception(ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, "ClickLake returned only unsupported files");

    auto pipe = Pipe::unitePipes(std::move(pipes));
    pipeline.init(std::move(pipe));
}


void StorageClickLake::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    auto storage_ptr = std::static_pointer_cast<StorageClickLake>(shared_from_this());
    auto read_step = std::make_unique<ReadFromClickLake>(
        column_names,
        std::move(storage_ptr),
        storage_snapshot,
        query_info,
        local_context,
        max_block_size);

    query_plan.addStep(std::move(read_step));
}


/// ClickLakeSink implementation

ClickLakeSink::ClickLakeSink(
    const String & server_url,
    const Block & sample_block,
    ContextPtr context_,
    const ConnectionTimeouts & timeouts_)
    : SinkToStorage(std::make_shared<const Block>(sample_block))
    , context(context_)
    , timeouts(timeouts_)
{
    Poco::URI insert_uri(server_url);
    insert_uri.setPath(insert_uri.getPath() + "/insert");
    insert_url = insert_uri.toString();
}

ClickLakeSink::~ClickLakeSink()
{
    try
    {
        if (arrow_writer)
            finalizeBuffers();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ClickLakeSink::initBuffers()
{
    if (initialized)
        return;

    Poco::URI uri(insert_url);

    http_buffer = BuilderWriteBufferFromHTTP(uri)
        .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withContentType("application/vnd.apache.arrow.stream")
        .withTimeouts(timeouts)
        .create();

    /// Create Arrow output format
    arrow_writer = FormatFactory::instance().getOutputFormat(
        "ArrowStream",
        *http_buffer,
        getHeader().getColumns().empty() ? Block() : getHeader(),
        context);

    initialized = true;
}

void ClickLakeSink::consume(Chunk & chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    initBuffers();

    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    arrow_writer->write(block);
}

void ClickLakeSink::onFinish()
{
    if (arrow_writer)
        finalizeBuffers();
}

void ClickLakeSink::finalizeBuffers()
{
    if (!arrow_writer)
        return;

    arrow_writer->finalize();
    arrow_writer->flush();
    arrow_writer.reset();

    if (http_buffer)
    {
        http_buffer->finalize();
        http_buffer.reset();
    }
}


SinkToStoragePtr StorageClickLake::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /*async_insert*/)
{
    return std::make_shared<ClickLakeSink>(
        server_url,
        metadata_snapshot->getSampleBlock(),
        local_context,
        getTimeouts(local_context));
}


void StorageClickLake::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN
            && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN
            && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Alter of type '{}' is not supported by storage {}", command.type, getName());
        }
    }
}


void StorageClickLake::alter(const AlterCommands & commands, ContextPtr local_context, AlterLockHolder &)
{
    /// Send mutation to server asynchronously
    std::ostringstream mutation_text;
    for (const auto & command : commands)
    {
        mutation_text << "ALTER " << command.type << " ";
        if (!command.column_name.empty())
            mutation_text << backQuoteIfNeed(command.column_name) << " ";
    }

    sendMutationToServer(mutation_text.str(), local_context);

    /// Update local metadata
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    commands.apply(new_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}


void registerStorageClickLake(StorageFactory & factory)
{
    factory.registerStorage("ClickLake", [](const StorageFactory::Arguments & args)
    {
        if (args.engine_args.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "ClickLake engine requires exactly 1 argument: server URL. "
                "Usage: ENGINE = ClickLake('http://server:port/table')");
        }

        String server_url = checkAndGetLiteralArgument<String>(args.engine_args[0], "server_url");

        /// Validate URL
        try
        {
            Poco::URI uri(server_url);
            if (uri.getScheme() != "http" && uri.getScheme() != "https")
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ClickLake server URL must use http or https scheme, got: {}", uri.getScheme());
            }
        }
        catch (const Poco::SyntaxException & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Invalid ClickLake server URL: {}", e.displayText());
        }

        return std::make_shared<StorageClickLake>(
            server_url,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext());
    },
    {
        .supports_parallel_insert = true,
        .supports_schema_inference = false,
    });
}

}
