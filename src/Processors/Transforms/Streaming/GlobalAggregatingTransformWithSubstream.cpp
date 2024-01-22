#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
GlobalAggregatingTransformWithSubstream::GlobalAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "GlobalAggregatingTransformWithSubstream",
        ProcessorID::GlobalAggregatingTransformWithSubstreamID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::OTHER);
    if (params->emit_changelog && params->emit_version)
        throw Exception(ErrorCodes::UNSUPPORTED, "'emit_version()' is not supported in global aggregation emit changelog");
}

SubstreamContextPtr GlobalAggregatingTransformWithSubstream::getOrCreateSubstreamContext(const SubstreamID & id)
{
    auto substream_ctx = AggregatingTransformWithSubstream::getOrCreateSubstreamContext(id);
    /// Need extra retracted data for old version impl
    if (params->emit_changelog && !substream_ctx->hasField())
    {
        substream_ctx->setField(
            {{},
            /// Field serializer
            [](const std::any &, WriteBuffer &, VersionType version) {
                assert(version < IMPL_V2_MIN_VERSION && "New impl doesn't need field");
            },
            /// Field deserializer
            [&variants = substream_ctx->variants, this](std::any &, ReadBuffer & rb, VersionType version) {
                assert(version < IMPL_V2_MIN_VERSION && "New impl doesn't need field");
                /// Convert old impl to new impl V2
                if (params->aggregator.expandedDataType() != ExpandedDataType::UpdatedWithRetracted)
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Failed to recover aggregation checkpoint. Recover old version '{}' checkpoint, checkpointed need retracted, but "
                        "current not need",
                        version);

                AggregatedDataVariants retracted;
                DB::deserialize(retracted, rb, params->aggregator);
                params->aggregator.mergeRetractedInto(variants, retracted);
            }});
    }
    return substream_ctx;
}

std::pair<bool, bool>
GlobalAggregatingTransformWithSubstream::executeOrMergeColumns(Chunk & chunk, const SubstreamContextPtr & substream_ctx)
{
    if (params->emit_changelog)
    {
        assert(!params->only_merge);
        auto num_rows = chunk.getNumRows();
        return params->aggregator.executeAndRetractOnBlock(
            chunk.detachColumns(), 0, num_rows, substream_ctx->variants, key_columns, aggregate_columns, no_more_keys);
    }
    else
        return AggregatingTransformWithSubstream::executeOrMergeColumns(chunk, substream_ctx);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransformWithSubstream::finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto finalized_watermark = chunk_ctx->getWatermark();
    SCOPE_EXIT({
        substream_ctx->resetRowCounts();
        substream_ctx->finalized_watermark = finalized_watermark;
    });

    /// If there is no new data, don't emit aggr result
    if (!substream_ctx->hasNewData())
        return;

    auto & variants = substream_ctx->variants;
    if (variants.empty())
        return;

    auto start = MonotonicMilliseconds::now();
    if (params->emit_changelog)
    {
        auto [retracted_chunk, chunk] = AggregatingHelper::convertToChangelogChunk(variants, *params);
        chunk.setChunkContext(chunk_ctx);
        setCurrentChunk(std::move(chunk), std::move(retracted_chunk));
    }
    else
    {
        auto chunk = AggregatingHelper::convertToChunk(variants, *params);
        if (params->final && params->emit_version)
            emitVersion(chunk, substream_ctx);

        chunk.setChunkContext(chunk_ctx);
        setCurrentChunk(std::move(chunk));
    }
    auto end = MonotonicMilliseconds::now();

    LOG_INFO(log, "Took {} milliseconds to finalize aggregation", end - start);
}

}
}
