// Row-at-a-time baseline: a classic Volcano iterator over an in-memory row
// store. One virtual next() call per row per operator, predicates evaluated
// through virtual expression objects, group-by via hash lookup per row.
// It runs the same plan (same dictionary codes) so only the execution model
// differs from the vectorized engine.
#pragma once
#include "exec.h"
#include "reader.h"

namespace cq {

struct RowStore {
  size_t nc = 0, n = 0;
  std::vector<int64_t> data;  // row-major
};

inline RowStore load_rows(const Table& t) {
  RowStore rs;
  rs.nc = t.cols.size();
  rs.n = t.nrows;
  rs.data.resize(rs.n * rs.nc);
  std::vector<int64_t> tmp(kBatch);
  size_t row0 = 0;
  for (size_t b = 0; b < t.nblocks; b++) {
    size_t rows = t.cols[0].blocks[b].rows;
    for (size_t c = 0; c < rs.nc; c++) {
      ColumnCursor cur;
      cur.init(t.base, t.cols[c].blocks[b]);
      for (size_t done = 0; done < rows;) {
        size_t got = cur.next(kBatch, tmp.data());
        for (size_t i = 0; i < got; i++) rs.data[(row0 + done + i) * rs.nc + c] = tmp[i];
        done += got;
      }
    }
    row0 += rows;
  }
  return rs;
}

class RowOp {
 public:
  virtual ~RowOp() = default;
  virtual const int64_t* next() = 0;
};

class RowScan : public RowOp {
 public:
  explicit RowScan(const RowStore& rs) : rs_(rs) {}
  const int64_t* next() override { return i_ < rs_.n ? &rs_.data[i_++ * rs_.nc] : nullptr; }

 private:
  const RowStore& rs_;
  size_t i_ = 0;
};

class RowExpr {
 public:
  virtual ~RowExpr() = default;
  virtual bool eval(const int64_t* row) const = 0;
};

class RowCompare : public RowExpr {
 public:
  explicit RowCompare(const Pred& p) : p_(p) {}
  bool eval(const int64_t* row) const override {
    int64_t a = row[p_.col], b = p_.val;
    switch (p_.op) {
      case CmpOp::Eq: return a == b;
      case CmpOp::Ne: return a != b;
      case CmpOp::Lt: return a < b;
      case CmpOp::Le: return a <= b;
      case CmpOp::Gt: return a > b;
      case CmpOp::Ge: return a >= b;
    }
    return false;
  }

 private:
  Pred p_;
};

class RowFilter : public RowOp {
 public:
  RowFilter(std::unique_ptr<RowOp> child, const Plan& p) : child_(std::move(child)) {
    for (auto& pr : p.preds) preds_.push_back(std::make_unique<RowCompare>(pr));
  }
  const int64_t* next() override {
    while (const int64_t* r = child_->next()) {
      bool ok = true;
      for (auto& e : preds_)
        if (!e->eval(r)) {
          ok = false;
          break;
        }
      if (ok) return r;
    }
    return nullptr;
  }

 private:
  std::unique_ptr<RowOp> child_;
  std::vector<std::unique_ptr<RowExpr>> preds_;
};

inline Result run_row_at_a_time(const Table& t, const RowStore& rs, const Plan& p) {
  AggState st(p.aggs.size());
  if (!p.always_false) {
    RowFilter root(std::make_unique<RowScan>(rs), p);
    std::unordered_map<int64_t, uint32_t> map;
    if (p.group_col < 0) st.add_group(0);
    while (const int64_t* r = root.next()) {
      uint32_t g = 0;
      if (p.group_col >= 0) {
        int64_t key = r[p.group_col];
        auto it = map.find(key);
        if (it == map.end()) it = map.emplace(key, st.add_group(key)).first;
        g = it->second;
      }
      st.cnt[g]++;
      for (size_t a = 0; a < p.aggs.size(); a++) {
        if (p.aggs[a].func == Func::Count) continue;
        int64_t x = r[p.aggs[a].col];
        st.sum[a][g] += x;
        st.mn[a][g] = std::min(st.mn[a][g], x);
        st.mx[a][g] = std::max(st.mx[a][g], x);
      }
    }
  } else if (p.group_col < 0) {
    st.add_group(0);
  }
  return finalize(t, p, st);
}

}  // namespace cq
