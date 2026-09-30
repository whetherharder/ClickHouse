#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <TableFunctions/ITableFunction.h>
#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif


namespace DB
{

namespace Setting
{
    extern const SettingsBool parallelize_output_from_storages;
    extern const SettingsBool s3_validate_etag_on_read;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}


ReadFromObjectStorageStep::ReadFromObjectStorageStep(
    const StorageID & storage_id_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const Names & columns_to_read,
    const NamesAndTypesList & virtual_columns_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const std::optional<DB::FormatSettings> & format_settings_,
    bool distributed_processing_,
    ReadFromFormatInfo info_,
    bool need_only_count_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t num_streams_)
    : SourceStepWithFilter(std::make_shared<const Block>(info_.source_header), columns_to_read, query_info_, storage_snapshot_, context_)
    , storage_id(storage_id_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , info(std::move(info_))
    , virtual_columns(virtual_columns_)
    , format_settings(format_settings_)
    , need_only_count(need_only_count_)
    , max_block_size(max_block_size_)
    , num_streams(num_streams_)
    , max_num_streams(num_streams_)
    , distributed_processing(distributed_processing_)
{
}

QueryPlanStepPtr ReadFromObjectStorageStep::clone() const
{
    return std::make_unique<ReadFromObjectStorageStep>(*this);
}

void ReadFromObjectStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    if (!filter_actions_dag)
        return;

    if (boost::iequals(configuration->format, "Parquet") || boost::iequals(configuration->format, "ORC"))
        prepareEagerKeyConditionSets(
            filter_actions_dag,
            storage_snapshot, info.source_header,
            query_info.prewhere_info, query_info.row_level_filter, getContext());

    // It is important to build the inplace sets for the filter here, before reading data from object storage.
    // If we delay building these sets until later in the pipeline, the filter can be applied after the data
    // has already been read, potentially in parallel across many streams. This can significantly reduce the
    // effectiveness of an Iceberg partition pruning, as unnecessary data may be read. Additionally, building ordered sets
    // at this stage enables the KeyCondition class to apply more efficient optimizations than for unordered sets.
    /// Idempotent — sets already built above are skipped via !future_set->get() check.
    VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(*filter_actions_dag, getContext());
}

void ReadFromObjectStorageStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    info = updateFormatPrewhereInfo(info, prewhere_info_value);
    query_info.prewhere_info = prewhere_info_value;
    output_header = std::make_shared<const Block>(info.source_header);
}

void ReadFromObjectStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    createIterator();
#if !CLICKHOUSE_CLOUD
    keepOnlyBucketOfDistributedRead(build_settings);
#endif

    Pipes pipes;
    auto context = getContext();
    size_t estimated_keys_count = iterator_wrapper->estimatedKeysCount();

    if (estimated_keys_count > 1)
        num_streams = std::min(num_streams, estimated_keys_count);
    else
    {
        /// The amount of keys (zero) was probably underestimated.
        /// We will keep one stream for this particular case.
        num_streams = 1;
    }

    // here create for node -> query -> level thread pool
    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), num_streams);

    auto format_filter_info = std::make_shared<FormatFilterInfo>(
        filter_actions_dag,
        context,
        configuration->getColumnMapperForCurrentSchema(storage_snapshot->metadata, context),
        query_info.row_level_filter,
        query_info.prewhere_info);

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<StorageObjectStorageSource>(
            storage_id,
            getName(),
            object_storage,
            configuration,
            storage_snapshot,
            info,
            format_settings,
            context,
            max_block_size,
            iterator_wrapper,
            parser_shared_resources,
            format_filter_info,
            need_only_count,
            lazy_row_index_registry);

        pipes.emplace_back(std::move(source));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    if (pipe.empty())
        pipe = Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(info.source_header)));

    size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef()[Setting::parallelize_output_from_storages];
    /// `max_num_streams` is a read-parallelism request, not a thread budget.
    const size_t resize_to = std::min(max_num_streams, build_settings.max_threads);
    if (parallelize_output
        && FormatFactory::instance().checkParallelizeOutputAfterReading(configuration->format, context)
        && output_ports > 0 && output_ports < resize_to)
        pipe.resize(resize_to);

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromObjectStorageStep::createIterator()
{
    if (iterator_wrapper)
        return;

    const ActionsDAG::Node * predicate = nullptr;
    if (filter_actions_dag)
        predicate = filter_actions_dag->getOutputs().at(0);

    auto context = getContext();

    iterator_wrapper = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(context), object_storage, storage_snapshot->metadata, distributed_processing,
        context, predicate, filter_actions_dag.get(), virtual_columns, info.hive_partition_columns_to_read_from_file_path, nullptr, context->getFileProgressCallback(),
        /*ignore_archive_globs=*/ false, /*skip_object_metadata=*/ false, /*with_tags=*/ info.requested_virtual_columns.contains("_tags"));
}

static InputOrderInfoPtr convertSortingKeyToInputOrder(const KeyDescription & key_description)
{
    SortDescription sort_description_for_merging;
    for (size_t i = 0; i < key_description.column_names.size(); ++i)
        sort_description_for_merging.push_back(
            SortColumnDescription(key_description.column_names[i], (!key_description.reverse_flags.empty() && key_description.reverse_flags[i]) ? -1 : 1));
    return std::make_shared<const InputOrderInfo>(sort_description_for_merging, sort_description_for_merging.size(), 1, 0);
}

bool ReadFromObjectStorageStep::canUseLazyMaterialization() const
{
    if (need_only_count)
        return false;

    /// The global row index requires per-row file row numbers (ChunkInfoRowNumbers), and the lazy
    /// branch requires reading an explicit set of rows (FormatFilterInfo::rows_to_read).
    /// Only the Parquet reader supports both.
    if (!boost::iequals(configuration->format, "Parquet"))
        return false;

    /// Data lakes can have per-file formats, deletes, and schema evolution; the configuration
    /// proves against the concrete data snapshot that every file can take the lazy path.
    if (!configuration->supportsLazyMaterialization(storage_snapshot->metadata, getContext()))
        return false;

    /// The lazy pass rereads the surviving files and must prove it sees the same generation of
    /// each object (see `LazyRowsObjectIterator::validateObjectGeneration`). A backend whose
    /// metadata may carry no comparable token at all (no `ETag`, unknown size and modification
    /// time — e.g. a web origin) would fail close on that reread even without any concurrent
    /// overwrite, so keep it on the single-pass plan.
    if (!object_storage->supportsObjectGenerationComparison())
        return false;

    /// Even when the two generations are comparable, on most backends the second pass opens an
    /// unconditional read: `AzureObjectStorage`, `HDFSObjectStorage` and the local disk ignore
    /// `StoredObject::etag`, so a concurrent in-place overwrite between the metadata probe and the
    /// read could still stitch together rows of two versions of the file. The reread is only
    /// generation-safe when either:
    ///   - the data files are immutable by the format's contract — a data lake never overwrites a
    ///     data file in place, so a file identified by path is always the same generation; or
    ///   - the backend pins the actual read to the captured generation — S3 with
    ///     `s3_validate_etag_on_read` issues the GET with an `If-Match` on the captured ETag and
    ///     rejects a response whose ETag drifted from it (see `ReadBufferFromS3`), which is atomic
    ///     with respect to an overwrite.
    /// The pin only takes effect when the captured metadata actually carries a non-empty `ETag`
    /// (see `createReadBuffer`), and `GCS` accessed through the S3 API is documented to legitimately
    /// return objects without one — so a `GCS`-provider client is not pinned even with the setting
    /// on. Any other provider that unexpectedly yields an empty `ETag` at read time fails close in
    /// `LazyRowsObjectIterator::validateObjectGeneration` instead of degrading to an unpinned read.
    /// For a mutable file on a backend that cannot pin the read, keep the single-pass plan.
    bool reread_is_generation_pinned = false;
#if USE_AWS_S3
    if (object_storage->getType() == ObjectStorageType::S3
        && getContext()->getSettingsRef()[Setting::s3_validate_etag_on_read])
    {
        const auto s3_client = object_storage->tryGetS3StorageClient();
        reread_is_generation_pinned = s3_client && s3_client->getProviderType() != S3::ProviderType::GCS;
    }
#endif
    if (!configuration->dataFilesAreImmutable() && !reread_is_generation_pinned)
        return false;

    /// The transformed plan is not serializable.
    if (distributed_read_bucket_count)
        return false;

    return true;
}

std::unique_ptr<LazilyReadFromObjectStorage> ReadFromObjectStorageStep::keepOnlyRequiredColumnsAndCreateLazyReadStep(const NameSet & required_names)
{
    /// A row policy is not part of `info`, but the source evaluates it in the main pass via
    /// `FormatFilterInfo`, so its input columns must not be deferred to the lazy branch.
    NameSet names_to_keep = required_names;
    if (query_info.row_level_filter)
        for (const auto & column : query_info.row_level_filter->actions.getRequiredColumns())
            names_to_keep.insert(column.name);

    auto lazy_info = splitLazilyReadColumnsFromFormatInfo(info, names_to_keep);
    if (!lazy_info)
        return {};

    output_header = std::make_shared<const Block>(info.source_header);

    NameSet lazy_names;
    for (const auto & column : lazy_info->source_header)
        lazy_names.insert(column.name);
    std::erase_if(required_source_columns, [&](const String & name) { return lazy_names.contains(name); });

    lazy_row_index_registry = std::make_shared<LazyObjectStorageFileRegistry>();

    auto lazy_source_header = std::make_shared<const Block>(lazy_info->source_header);
    auto lazy_step = std::make_unique<LazilyReadFromObjectStorage>(
        std::move(lazy_source_header),
        storage_id,
        object_storage,
        configuration,
        storage_snapshot,
        format_settings,
        std::move(*lazy_info),
        getContext(),
        max_block_size);

    return lazy_step;
}

bool ReadFromObjectStorageStep::requestReadingInOrder() const
{
    return configuration->isDataSortedBySortingKey(storage_snapshot->metadata, getContext());
}

InputOrderInfoPtr ReadFromObjectStorageStep::getDataOrder() const
{
    return convertSortingKeyToInputOrder(storage_snapshot->metadata->getSortingKey());
}

/// ClickHouse Cloud has its own implementation of the distributed read.
#if !CLICKHOUSE_CLOUD

namespace
{

/// Every worker enumerates the same pinned table state, so choosing by object identity alone
/// makes the buckets disjoint and complete without shipping file lists.
class ObjectIteratorOfBucket : public IObjectIterator
{
public:
    ObjectIteratorOfBucket(ObjectIterator iterator_, UInt64 bucket_, UInt64 total_buckets_)
        : iterator(std::move(iterator_)), bucket(bucket_), total_buckets(total_buckets_)
    {
    }

    ObjectInfoPtr next(size_t processor) override
    {
        while (auto object = iterator->next(processor))
            if (sipHash64(object->getIdentifier()) % total_buckets == bucket)
                return object;
        return nullptr;
    }

    size_t estimatedKeysCount() override { return iterator->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return iterator->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        iterator->setEmitProfileEvents(value);
    }

private:
    const ObjectIterator iterator;
    const UInt64 bucket;
    const UInt64 total_buckets;
};

std::optional<DataLakeTableStateSnapshot> getIcebergTableState(const StorageSnapshotPtr & storage_snapshot)
{
    const auto & state = storage_snapshot->metadata->datalake_table_state;
    if (!state || !std::holds_alternative<Iceberg::TableStateSnapshot>(*state))
        return {};
    return state;
}

}

void ReadFromObjectStorageStep::setDistributedRead(size_t bucket_count)
{
    distributed_read_bucket_count = bucket_count;
}

void ReadFromObjectStorageStep::keepOnlyBucketOfDistributedRead(const BuildQueryPipelineSettings & build_settings)
{
    if (distributed_read_bucket_count == 0)
        return;

    if (!build_settings.parameter_lookup)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "A bucketed read from {} is executed outside of a distributed plan task", storage_id.getNameForLogs());

    const UInt64 bucket = parse<UInt64>(build_settings.parameter_lookup->getParameter("bucket_id").safeGet<String>());
    const UInt64 total_buckets = build_settings.parameter_lookup->getParameter("total_buckets").safeGet<UInt64>();
    if (total_buckets != distributed_read_bucket_count || bucket >= total_buckets)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bucket {} of {} does not match the distributed read from {} into {} buckets",
            bucket, total_buckets, storage_id.getNameForLogs(), distributed_read_bucket_count);

    iterator_wrapper = std::make_shared<ObjectIteratorOfBucket>(std::move(iterator_wrapper), bucket, total_buckets);
}

Strings ReadFromObjectStorageStep::getShardsForDistributedRead() const
{
    if (distributed_read_bucket_count == 0)
        return {"0"};

    Strings list_of_shards;
    for (size_t i = 0; i < distributed_read_bucket_count; ++i)
        list_of_shards.push_back(std::to_string(i));
    return list_of_shards;
}

bool ReadFromObjectStorageStep::isSerializable() const
{
    /// A worker resolves the table by name, which a table function does not have.
    return !distributed_processing
        && !lazy_row_index_registry
        && storage_id.hasDatabase()
        && storage_id.database_name != ITableFunction::getDatabaseName()
        && configuration->isDataLakeConfiguration()
        && getIcebergTableState(storage_snapshot).has_value();
}

std::optional<size_t> ReadFromObjectStorageStep::totalRowsInSnapshot() const
{
    return configuration->totalRows(getContext());
}

void ReadFromObjectStorageStep::serialize(Serialization & ctx) const
{
    const auto state = getIcebergTableState(storage_snapshot);
    if (!isSerializable() || !state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading from {} cannot be serialized", storage_id.getNameForLogs());

    writeStringBinary(storage_id.getDatabaseName(), ctx.out);
    writeStringBinary(storage_id.getTableName(), ctx.out);
    writeVarUInt(required_source_columns.size(), ctx.out);
    for (const auto & column : required_source_columns)
        writeStringBinary(column, ctx.out);

    writeVarUInt(max_block_size, ctx.out);
    writeVarUInt(max_num_streams, ctx.out);

    UInt8 flags = 0;
    if (need_only_count)
        flags |= 1;
    if (query_info.row_level_filter)
        flags |= 2;
    if (query_info.prewhere_info)
        flags |= 4;
    writeIntBinary(flags, ctx.out);

    if (query_info.row_level_filter)
        query_info.row_level_filter->serialize(ctx);
    if (query_info.prewhere_info)
        query_info.prewhere_info->serialize(ctx);

    serializeDataLakeTableStateSnapshot(*state, ctx.out);
    writeVarUInt(distributed_read_bucket_count, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ReadFromObjectStorageStep::deserialize(Deserialization & ctx)
{
    String database_name;
    String table_name;
    readStringBinary(database_name, ctx.in);
    readStringBinary(table_name, ctx.in);

    size_t num_columns = 0;
    readVarUInt(num_columns, ctx.in);
    Names column_names(num_columns);
    for (auto & column : column_names)
        readStringBinary(column, ctx.in);

    UInt64 max_block_size = 0;
    readVarUInt(max_block_size, ctx.in);
    UInt64 num_streams = 0;
    readVarUInt(num_streams, ctx.in);

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    SelectQueryInfo query_info;
    query_info.optimize_trivial_count = flags & 1;
    if (flags & 2)
        query_info.row_level_filter = std::make_shared<FilterDAGInfo>(FilterDAGInfo::deserialize(ctx));
    if (flags & 4)
        query_info.prewhere_info = std::make_shared<PrewhereInfo>(PrewhereInfo::deserialize(ctx));

    auto state = deserializeDataLakeTableStateSnapshot(ctx.in);
    size_t distributed_read_bucket_count = 0;
    readVarUInt(distributed_read_bucket_count, ctx.in);

    if (ctx.skipping)
        return std::make_unique<ReadNothingStep>(ctx.output_header);

    /// Same as for `ReadFromMergeTree`: a shipped plan does not carry the limits of the read.
    auto storage_limits = std::make_shared<StorageLimitsList>();
    storage_limits->emplace_back(buildStorageLimits(*ctx.context, SelectQueryOptions(QueryProcessingStage::FetchColumns)));
    query_info.storage_limits = std::move(storage_limits);

    /// The table could be dropped concurrently after the plan was serialized.
    StorageID table_id(database_name, table_name);
    auto storage = DatabaseCatalog::instance().getTable(table_id, ctx.context);
    auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get());
    if (!object_storage_table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} is not an object storage table", table_id.getNameForLogs());
    ctx.storage_holders.push_back(storage);

    if (ctx.context->hasQueryContext())
        ctx.context->getQueryContext()->addQueryAccessInfo(storage->getStorageID(), column_names);

    /// Loads metadata at least as new as the initiator's, so the pinned schema is known here.
    object_storage_table->updateExternalDynamicMetadataIfExists(ctx.context);
    const auto metadata_snapshot = object_storage_table->getInMemoryMetadataPtr(ctx.context, false);
    auto storage_snapshot = object_storage_table->getStorageSnapshot(metadata_snapshot, ctx.context);
    storage_snapshot = object_storage_table->getStorageSnapshotForTableState(storage_snapshot, state, ctx.context);

    QueryPlan plan;
    object_storage_table->read(
        plan, column_names, storage_snapshot, query_info, ctx.context, QueryProcessingStage::FetchColumns, max_block_size, num_streams);

    auto * root = plan.getRootNode();
    auto * step = root ? typeid_cast<ReadFromObjectStorageStep *>(root->step.get()) : nullptr;
    if (!step || !root->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Reading from {} did not produce a single ReadFromObjectStorage step", table_id.getNameForLogs());

    step->setDistributedRead(distributed_read_bucket_count);
    return std::move(root->step);
}

void registerReadFromObjectStorageStep(QueryPlanStepRegistry & registry);
void registerReadFromObjectStorageStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep(ReadFromObjectStorageStep::STEP_NAME, ReadFromObjectStorageStep::deserialize);
}

#endif

}
