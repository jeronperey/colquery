#pragma once
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include "format.h"

namespace cq {

// Memory-mapped, read-only view of a CQF file.
class Table {
 public:
  explicit Table(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open " + path);
    struct stat st;
    fstat(fd, &st);
    size = (size_t)st.st_size;
    void* p = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed");
    base = (const uint8_t*)p;
    if (size < 16 || memcmp(base, kMagic, 8) != 0) throw std::runtime_error("not a CQF file");
    uint64_t footer;
    memcpy(&footer, base + 8, 8);
    const uint8_t* q = base + footer;
    auto get = [&](void* dst, size_t n) {
      memcpy(dst, q, n);
      q += n;
    };
    get(&nrows, 8);
    get(&source_bytes, 8);
    uint32_t nc;
    get(&nc, 4);
    cols.resize(nc);
    for (auto& c : cols) {
      uint16_t nl;
      get(&nl, 2);
      c.name.assign((const char*)q, nl);
      q += nl;
      get(&c.type, 1);
      uint32_t nd;
      get(&nd, 4);
      c.dict.resize(nd);
      for (auto& s : c.dict) {
        uint32_t l;
        get(&l, 4);
        s.assign((const char*)q, l);
        q += l;
      }
      uint32_t nb;
      get(&nb, 4);
      c.blocks.resize(nb);
      for (auto& b : c.blocks) get(&b, sizeof(b));
    }
    nblocks = cols.empty() ? 0 : cols[0].blocks.size();
  }
  ~Table() { munmap((void*)base, size); }
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  int col_index(const std::string& name) const {
    for (size_t i = 0; i < cols.size(); i++)
      if (cols[i].name == name) return (int)i;
    return -1;
  }

  const uint8_t* base = nullptr;
  size_t size = 0;
  uint64_t nrows = 0, source_bytes = 0;
  size_t nblocks = 0;
  std::vector<Column> cols;
};

// Streams decoded int64 values out of one (column, block) chunk.
class ColumnCursor {
 public:
  void init(const uint8_t* base, const BlockMeta& m) {
    m_ = &m;
    p_ = base + m.offset;
    pos_ = 0;
    run_ = 0;
    run_off_ = 0;
    if (m.enc == (uint8_t)Enc::Rle) {
      rv_ = (const int64_t*)p_;
      rl_ = (const uint32_t*)(p_ + (size_t)m.nruns * 8);
    }
  }

  // Decodes up to n values into out; returns how many were produced.
  size_t next(size_t n, int64_t* out) {
    n = std::min<size_t>(n, m_->rows - pos_);
    if (m_->enc == (uint8_t)Enc::Plain) {
      switch (m_->width) {
        case 1: unpack<uint8_t>(n, out); break;
        case 2: unpack<uint16_t>(n, out); break;
        case 4: unpack<uint32_t>(n, out); break;
        default: unpack<uint64_t>(n, out); break;
      }
    } else {
      size_t filled = 0;
      while (filled < n) {
        size_t take = std::min<size_t>(n - filled, rl_[run_] - run_off_);
        std::fill(out + filled, out + filled + take, rv_[run_]);
        filled += take;
        run_off_ += (uint32_t)take;
        if (run_off_ == rl_[run_]) {
          run_++;
          run_off_ = 0;
        }
      }
    }
    pos_ += n;
    return n;
  }

 private:
  template <class T>
  void unpack(size_t n, int64_t* out) {
    const uint8_t* src = p_ + pos_ * sizeof(T);
    const uint64_t base = (uint64_t)m_->min;
    for (size_t i = 0; i < n; i++) {
      T x;
      memcpy(&x, src + i * sizeof(T), sizeof(T));
      out[i] = (int64_t)(base + (uint64_t)x);
    }
  }

  const BlockMeta* m_ = nullptr;
  const uint8_t* p_ = nullptr;
  size_t pos_ = 0;
  const int64_t* rv_ = nullptr;
  const uint32_t* rl_ = nullptr;
  size_t run_ = 0;
  uint32_t run_off_ = 0;
};

}  // namespace cq
