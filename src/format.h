// On-disk format for CQF ("columnar query file").
//
//   [magic 8B][footer offset 8B]
//   [column chunk]...            one chunk per (column, block), 8-byte aligned
//   [footer]                     schema, dictionaries, per-block metadata + zone maps
//
// Each column is split into blocks of kBlockRows rows. Per block we pick the
// smaller of two encodings:
//   Plain: frame-of-reference + narrowest fixed width (1/2/4/8 bytes) of (v - min)
//   Rle:   run values (int64[nruns]) followed by run lengths (uint32[nruns])
// String columns are dictionary encoded: the dictionary is sorted, so integer
// code order equals lexicographic order and range predicates / zone maps work
// directly on codes.
#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cq {

constexpr size_t kBatch = 1024;       // rows per vectorized batch
constexpr size_t kBlockRows = 65536;  // rows per storage block (64 batches)
constexpr size_t kMaxCols = 16;
constexpr char kMagic[8] = {'C', 'Q', 'F', '1', 0, 0, 0, 0};

enum class ColType : uint8_t { Int = 0, Str = 1 };
enum class Enc : uint8_t { Plain = 0, Rle = 1 };

struct BlockMeta {
  uint64_t offset;
  uint32_t bytes;
  uint32_t rows;
  int64_t min, max;  // zone map
  uint8_t enc;
  uint8_t width;  // bytes per value for Plain
  uint16_t pad;
  uint32_t nruns;
};
static_assert(sizeof(BlockMeta) == 40, "BlockMeta layout");

struct Column {
  std::string name;
  ColType type = ColType::Int;
  std::vector<std::string> dict;  // sorted; Str columns only
  std::vector<BlockMeta> blocks;
};

}  // namespace cq
