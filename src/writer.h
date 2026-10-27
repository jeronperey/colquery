#pragma once
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include "format.h"

namespace cq {

// Chooses an encoding for one block of values and serializes it into `out`.
inline void encode_block(const int64_t* v, size_t n, std::vector<uint8_t>& out, BlockMeta& m) {
  int64_t mn = v[0], mx = v[0];
  uint32_t runs = 1;
  for (size_t i = 1; i < n; i++) {
    mn = v[i] < mn ? v[i] : mn;
    mx = v[i] > mx ? v[i] : mx;
    runs += v[i] != v[i - 1];
  }
  uint64_t range = (uint64_t)mx - (uint64_t)mn;
  uint8_t w = range <= 0xFF ? 1 : range <= 0xFFFF ? 2 : range <= 0xFFFFFFFFull ? 4 : 8;
  size_t plain = n * w, rle = (size_t)runs * 12;

  m.min = mn;
  m.max = mx;
  m.rows = (uint32_t)n;
  m.nruns = runs;
  m.pad = 0;
  if (rle < plain / 2) {  // RLE only when clearly worth the slower decode
    m.enc = (uint8_t)Enc::Rle;
    m.width = 8;
    out.resize(rle);
    uint8_t* vals = out.data();
    uint8_t* lens = out.data() + (size_t)runs * 8;
    size_t r = 0, start = 0;
    for (size_t i = 1; i <= n; i++) {
      if (i == n || v[i] != v[start]) {
        uint32_t len = (uint32_t)(i - start);
        memcpy(vals + r * 8, &v[start], 8);
        memcpy(lens + r * 4, &len, 4);
        r++;
        start = i;
      }
    }
  } else {
    m.enc = (uint8_t)Enc::Plain;
    m.width = w;
    out.resize(plain);
    for (size_t i = 0; i < n; i++) {
      uint64_t x = (uint64_t)v[i] - (uint64_t)mn;
      memcpy(out.data() + i * w, &x, w);  // little-endian truncation
    }
  }
  m.bytes = (uint32_t)out.size();
}

class Writer {
 public:
  Writer(const std::string& path, std::vector<Column> schema) : cols_(std::move(schema)) {
    f_ = fopen(path.c_str(), "wb");
    if (!f_) throw std::runtime_error("cannot open " + path);
    setvbuf(f_, nullptr, _IOFBF, 1 << 20);
    put(kMagic, 8);
    uint64_t placeholder = 0;
    put(&placeholder, 8);
  }
  ~Writer() {
    if (f_) fclose(f_);
  }

  // One entry per column, all the same length (<= kBlockRows). String columns
  // are passed as dictionary codes.
  void append_block(const std::vector<std::vector<int64_t>>& cols) {
    if (cols.size() != cols_.size()) throw std::runtime_error("column count mismatch");
    for (size_t c = 0; c < cols.size(); c++) {
      BlockMeta m{};
      encode_block(cols[c].data(), cols[c].size(), buf_, m);
      pad8();
      m.offset = pos_;
      put(buf_.data(), buf_.size());
      cols_[c].blocks.push_back(m);
    }
    nrows_ += cols[0].size();
  }

  void finish(uint64_t source_bytes) {
    pad8();
    uint64_t footer = pos_;
    put(&nrows_, 8);
    put(&source_bytes, 8);
    uint32_t nc = (uint32_t)cols_.size();
    put(&nc, 4);
    for (auto& c : cols_) {
      uint16_t nl = (uint16_t)c.name.size();
      put(&nl, 2);
      put(c.name.data(), nl);
      put(&c.type, 1);
      uint32_t nd = (uint32_t)c.dict.size();
      put(&nd, 4);
      for (auto& s : c.dict) {
        uint32_t l = (uint32_t)s.size();
        put(&l, 4);
        put(s.data(), l);
      }
      uint32_t nb = (uint32_t)c.blocks.size();
      put(&nb, 4);
      for (auto& b : c.blocks) put(&b, sizeof(b));
    }
    fseek(f_, 8, SEEK_SET);
    fwrite(&footer, 8, 1, f_);
    fclose(f_);
    f_ = nullptr;
  }

 private:
  void put(const void* p, size_t n) {
    if (n && fwrite(p, 1, n, f_) != n) throw std::runtime_error("write failed");
    pos_ += n;
  }
  void pad8() {
    static const uint8_t zeros[8] = {};
    size_t r = pos_ % 8;
    if (r) put(zeros, 8 - r);
  }

  FILE* f_ = nullptr;
  uint64_t pos_ = 0, nrows_ = 0;
  std::vector<Column> cols_;
  std::vector<uint8_t> buf_;
};

}  // namespace cq
