// Vectorized execution: Scan -> Filter -> Aggregate over 1024-row batches.
// Filters produce selection vectors with branchless compaction; aggregates run
// tight per-column loops over the selected rows.
#pragma once
#include <climits>
#include <cstdio>
#include <map>
#include <memory>
#include <unordered_map>

#include "plan.h"

namespace cq {

struct ScanStats {
  size_t blocks_total = 0, blocks_scanned = 0, blocks_skipped = 0;
  uint64_t rows_scanned = 0;
};

struct Result {
  std::vector<std::string> header;
  std::vector<std::vector<std::string>> rows;

  std::string to_string() const {
    std::vector<size_t> w(header.size());
    for (size_t i = 0; i < header.size(); i++) w[i] = header[i].size();
    for (auto& r : rows)
      for (size_t i = 0; i < r.size(); i++) w[i] = std::max(w[i], r[i].size());
    std::string s;
    auto line = [&](const std::vector<std::string>& r) {
      for (size_t i = 0; i < r.size(); i++) {
        s += r[i];
        s.append(w[i] - r[i].size() + (i + 1 < r.size() ? 2 : 0), ' ');
      }
      s += '\n';
    };
    line(header);
    for (auto& r : rows) line(r);
    return s;
  }
  bool operator==(const Result& o) const { return header == o.header && rows == o.rows; }
};

// Per-group aggregate state shared by every engine so results are formatted identically.
struct AggState {
  std::vector<int64_t> keys, cnt;
  std::vector<std::vector<int64_t>> sum, mn, mx;  // [agg][group]

  explicit AggState(size_t naggs) : sum(naggs), mn(naggs), mx(naggs) {}
  uint32_t add_group(int64_t key) {
    keys.push_back(key);
    cnt.push_back(0);
    for (auto& v : sum) v.push_back(0);
    for (auto& v : mn) v.push_back(INT64_MAX);
    for (auto& v : mx) v.push_back(INT64_MIN);
    return (uint32_t)keys.size() - 1;
  }
};

inline Result finalize(const Table& t, const Plan& p, const AggState& st) {
  Result r;
  for (auto& o : p.out) r.header.push_back(o.header);
  std::vector<size_t> order;
  for (size_t g = 0; g < st.keys.size(); g++)
    if (st.cnt[g] > 0 || p.group_col < 0) order.push_back(g);
  std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return st.keys[a] < st.keys[b]; });
  for (size_t g : order) {
    std::vector<std::string> row;
    for (auto& o : p.out) {
      if (o.is_group) {
        const Column& c = t.cols[p.group_col];
        row.push_back(c.type == ColType::Str ? c.dict[st.keys[g]] : std::to_string(st.keys[g]));
        continue;
      }
      const AggSpec& a = p.aggs[o.agg];
      int64_t n = st.cnt[g];
      char buf[64];
      switch (a.func) {
        case Func::Count: row.push_back(std::to_string(n)); break;
        case Func::Sum: row.push_back(n ? std::to_string(st.sum[o.agg][g]) : "NULL"); break;
        case Func::Min: row.push_back(n ? std::to_string(st.mn[o.agg][g]) : "NULL"); break;
        case Func::Max: row.push_back(n ? std::to_string(st.mx[o.agg][g]) : "NULL"); break;
        case Func::Avg:
          snprintf(buf, sizeof buf, "%.4f", (double)st.sum[o.agg][g] / (double)n);
          row.push_back(n ? buf : "NULL");
          break;
      }
    }
    r.rows.push_back(std::move(row));
  }
  return r;
}

// 0 = no row in [mn, mx] can match, 1 = some may, 2 = every row matches.
inline int zone_test(const Pred& p, int64_t mn, int64_t mx) {
  int64_t v = p.val;
  switch (p.op) {
    case CmpOp::Eq: return (v < mn || v > mx) ? 0 : (mn == mx ? 2 : 1);
    case CmpOp::Ne: return (v < mn || v > mx) ? 2 : (mn == mx ? 0 : 1);
    case CmpOp::Lt: return mn >= v ? 0 : (mx < v ? 2 : 1);
    case CmpOp::Le: return mn > v ? 0 : (mx <= v ? 2 : 1);
    case CmpOp::Gt: return mx <= v ? 0 : (mn > v ? 2 : 1);
    case CmpOp::Ge: return mx < v ? 0 : (mn >= v ? 2 : 1);
  }
  return 1;
}

struct Batch {
  size_t n = 0;                  // rows decoded
  int64_t* col[kMaxCols] = {};   // indexed by table column
  int64_t store[kMaxCols][kBatch];
  uint16_t sel[kBatch];
  size_t nsel = 0;
  bool use_sel = false;          // false: all n rows are live
  uint32_t skip_mask = 0;        // predicates zone maps proved true for this block
};

class Op {
 public:
  virtual ~Op() = default;
  virtual Batch* next() = 0;
};

class ScanOp : public Op {
 public:
  ScanOp(const Table& t, const Plan& p, bool zone_maps, ScanStats& st)
      : t_(t), p_(p), zm_(zone_maps), st_(st), batch_(new Batch) {
    cur_.resize(p.scan_cols.size());
    st_.blocks_total = t.nblocks;
  }

  Batch* next() override {
    for (;;) {
      if (remaining_ > 0) {
        size_t n = std::min(kBatch, remaining_);
        for (size_t k = 0; k < cur_.size(); k++) {
          int c = p_.scan_cols[k];
          cur_[k].next(n, batch_->store[c]);
          batch_->col[c] = batch_->store[c];
        }
        remaining_ -= n;
        batch_->n = batch_->nsel = n;
        batch_->use_sel = false;
        batch_->skip_mask = skip_;
        st_.rows_scanned += n;
        return batch_.get();
      }
      if (block_ >= t_.nblocks) return nullptr;
      size_t b = block_++;
      skip_ = 0;
      bool prune = false;
      if (zm_) {
        for (size_t i = 0; i < p_.preds.size(); i++) {
          const BlockMeta& m = t_.cols[p_.preds[i].col].blocks[b];
          int r = zone_test(p_.preds[i], m.min, m.max);
          if (r == 0) {
            prune = true;
            break;
          }
          if (r == 2) skip_ |= 1u << i;
        }
      }
      if (prune) {
        st_.blocks_skipped++;
        continue;
      }
      st_.blocks_scanned++;
      for (size_t k = 0; k < cur_.size(); k++) cur_[k].init(t_.base, t_.cols[p_.scan_cols[k]].blocks[b]);
      remaining_ = t_.cols[p_.scan_cols[0]].blocks[b].rows;
    }
  }

 private:
  const Table& t_;
  const Plan& p_;
  bool zm_;
  ScanStats& st_;
  std::unique_ptr<Batch> batch_;
  std::vector<ColumnCursor> cur_;
  size_t block_ = 0, remaining_ = 0;
  uint32_t skip_ = 0;
};

namespace kernel {

template <CmpOp OP>
inline bool cmp(int64_t a, int64_t b) {
  if constexpr (OP == CmpOp::Eq) return a == b;
  else if constexpr (OP == CmpOp::Ne) return a != b;
  else if constexpr (OP == CmpOp::Lt) return a < b;
  else if constexpr (OP == CmpOp::Le) return a <= b;
  else if constexpr (OP == CmpOp::Gt) return a > b;
  else return a >= b;
}

// Branchless selection-vector builders. out may alias in (k never passes i).
template <CmpOp OP>
size_t select_dense(const int64_t* v, size_t n, int64_t c, uint16_t* out) {
  size_t k = 0;
  for (size_t i = 0; i < n; i++) {
    out[k] = (uint16_t)i;
    k += cmp<OP>(v[i], c);
  }
  return k;
}
template <CmpOp OP>
size_t select_sparse(const int64_t* v, const uint16_t* in, size_t m, int64_t c, uint16_t* out) {
  size_t k = 0;
  for (size_t i = 0; i < m; i++) {
    uint16_t j = in[i];
    out[k] = j;
    k += cmp<OP>(v[j], c);
  }
  return k;
}

inline size_t apply(const Pred& p, Batch& b) {
  const int64_t* v = b.col[p.col];
#define CQ_CASE(OPV)                                                                   \
  case CmpOp::OPV:                                                                     \
    return b.use_sel ? select_sparse<CmpOp::OPV>(v, b.sel, b.nsel, p.val, b.sel)       \
                     : select_dense<CmpOp::OPV>(v, b.n, p.val, b.sel);
  switch (p.op) {
    CQ_CASE(Eq) CQ_CASE(Ne) CQ_CASE(Lt) CQ_CASE(Le) CQ_CASE(Gt) CQ_CASE(Ge)
  }
#undef CQ_CASE
  return 0;
}

}  // namespace kernel

class FilterOp : public Op {
 public:
  FilterOp(std::unique_ptr<Op> child, const Plan& p) : child_(std::move(child)), p_(p) {}

  Batch* next() override {
    while (Batch* b = child_->next()) {
      for (size_t i = 0; i < p_.preds.size() && b->nsel > 0; i++) {
        if (b->skip_mask >> i & 1) continue;  // zone map says whole block satisfies it
        b->nsel = kernel::apply(p_.preds[i], *b);
        b->use_sel = true;
      }
      if (b->nsel > 0) return b;
    }
    return nullptr;
  }

 private:
  std::unique_ptr<Op> child_;
  const Plan& p_;
};

class AggregateOp {
 public:
  AggregateOp(std::unique_ptr<Op> child, const Plan& p, const Table& t)
      : child_(std::move(child)), p_(p), st_(p.aggs.size()) {
    if (p.group_col < 0) {
      st_.add_group(0);
    } else if (t.cols[p.group_col].type == ColType::Str) {
      dict_groups_ = true;  // codes are dense: group id == code
      for (size_t i = 0; i < t.cols[p.group_col].dict.size(); i++) st_.add_group((int64_t)i);
    }
  }

  AggState run() {
    while (Batch* b = child_->next()) consume(*b);
    return std::move(st_);
  }

 private:
  void consume(Batch& b) {
    size_t m = b.use_sel ? b.nsel : b.n;
    if (p_.group_col < 0) {
      st_.cnt[0] += (int64_t)m;
      for (size_t a = 0; a < p_.aggs.size(); a++) {
        if (p_.aggs[a].func == Func::Count) continue;
        const int64_t* v = b.col[p_.aggs[a].col];
        int64_t s = 0, lo = INT64_MAX, hi = INT64_MIN;
        if (!b.use_sel) {
          for (size_t i = 0; i < m; i++) {
            s += v[i];
            lo = v[i] < lo ? v[i] : lo;
            hi = v[i] > hi ? v[i] : hi;
          }
        } else {
          for (size_t i = 0; i < m; i++) {
            int64_t x = v[b.sel[i]];
            s += x;
            lo = x < lo ? x : lo;
            hi = x > hi ? x : hi;
          }
        }
        st_.sum[a][0] += s;
        st_.mn[a][0] = std::min(st_.mn[a][0], lo);
        st_.mx[a][0] = std::max(st_.mx[a][0], hi);
      }
      return;
    }
    const int64_t* key = b.col[p_.group_col];
    uint32_t gid[kBatch];
    uint16_t idx[kBatch];
    for (size_t k = 0; k < m; k++) {
      uint16_t i = b.use_sel ? b.sel[k] : (uint16_t)k;
      idx[k] = i;
      gid[k] = dict_groups_ ? (uint32_t)key[i] : lookup(key[i]);
    }
    for (size_t k = 0; k < m; k++) st_.cnt[gid[k]]++;
    for (size_t a = 0; a < p_.aggs.size(); a++) {
      if (p_.aggs[a].func == Func::Count) continue;
      const int64_t* v = b.col[p_.aggs[a].col];
      int64_t* sum = st_.sum[a].data();
      int64_t* mn = st_.mn[a].data();
      int64_t* mx = st_.mx[a].data();
      for (size_t k = 0; k < m; k++) {
        int64_t x = v[idx[k]];
        uint32_t g = gid[k];
        sum[g] += x;
        mn[g] = x < mn[g] ? x : mn[g];
        mx[g] = x > mx[g] ? x : mx[g];
      }
    }
  }

  uint32_t lookup(int64_t key) {
    auto it = map_.find(key);
    if (it != map_.end()) return it->second;
    uint32_t g = st_.add_group(key);
    map_.emplace(key, g);
    return g;
  }

  std::unique_ptr<Op> child_;
  const Plan& p_;
  AggState st_;
  bool dict_groups_ = false;
  std::unordered_map<int64_t, uint32_t> map_;
};

inline Result run_vectorized(const Table& t, const Plan& p, bool zone_maps, ScanStats* stats = nullptr) {
  ScanStats local;
  ScanStats& st = stats ? *stats : local;
  st = ScanStats{};
  st.blocks_total = t.nblocks;
  if (p.always_false) {
    st.blocks_skipped = t.nblocks;
    AggState empty(p.aggs.size());
    if (p.group_col < 0) empty.add_group(0);
    return finalize(t, p, empty);
  }
  AggregateOp agg(std::make_unique<FilterOp>(std::make_unique<ScanOp>(t, p, zone_maps, st), p), p, t);
  AggState s = agg.run();
  return finalize(t, p, s);
}

}  // namespace cq
