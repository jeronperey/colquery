// Planner: resolves names against the table, then applies three optimizations:
//   1. string literals are rewritten into dictionary-code predicates (and
//      predicates the dictionary proves always-true/always-false are folded away)
//   2. column pruning: only columns the query touches are scanned
//   3. predicates are ordered cheapest/most selective first (= before ranges before !=)
// Predicates stay attached to the scan so it can consult zone maps (pushdown).
#pragma once
#include <algorithm>
#include <sstream>

#include "reader.h"
#include "sql.h"

namespace cq {

struct Pred {
  int col;
  CmpOp op;
  int64_t val;
};

struct AggSpec {
  Func func;
  int col;  // -1 for COUNT(*)
};

struct OutItem {
  bool is_group;
  int agg;  // index into Plan::aggs when !is_group
  std::string header;
};

struct Plan {
  std::vector<Pred> preds;
  std::vector<int> scan_cols;
  int group_col = -1;
  std::vector<AggSpec> aggs;
  std::vector<OutItem> out;
  bool always_false = false;
};

inline const char* op_str(CmpOp o) {
  static const char* s[] = {"=", "!=", "<", "<=", ">", ">="};
  return s[(int)o];
}
inline const char* func_str(Func f) {
  static const char* s[] = {"count", "sum", "min", "max", "avg"};
  return s[(int)f];
}

inline Plan make_plan(const Query& q, const Table& t) {
  Plan p;
  auto resolve = [&](const std::string& name) {
    int c = t.col_index(name);
    if (c < 0) throw std::runtime_error("unknown column " + name);
    return c;
  };
  if (q.where.size() > 32) throw std::runtime_error("too many predicates");

  if (!q.group_by.empty()) p.group_col = resolve(q.group_by);
  bool any_agg = false;
  for (auto& it : q.items) {
    if (!it.is_agg) {
      if (q.group_by.empty() || it.col != q.group_by)
        throw std::runtime_error("column " + it.col + " must appear in GROUP BY");
      p.out.push_back({true, -1, it.col});
      continue;
    }
    any_agg = true;
    AggSpec a{it.func, -1};
    if (!it.star) {
      a.col = resolve(it.col);
      if (it.func != Func::Count && t.cols[a.col].type == ColType::Str)
        throw std::runtime_error(std::string(func_str(it.func)) + " requires a numeric column");
    }
    p.aggs.push_back(a);
    p.out.push_back({false, (int)p.aggs.size() - 1,
                     std::string(func_str(it.func)) + "(" + (it.star ? "*" : it.col) + ")"});
  }
  if (!any_agg) throw std::runtime_error("query must contain an aggregate");

  for (auto& c : q.where) {
    int col = resolve(c.col);
    const Column& cd = t.cols[col];
    if (cd.type == ColType::Int) {
      if (c.is_str) throw std::runtime_error("column " + c.col + " is numeric");
      p.preds.push_back({col, c.op, c.ival});
      continue;
    }
    if (!c.is_str) throw std::runtime_error("column " + c.col + " is a string");
    // Sorted dictionary: translate the literal to a code (or a code boundary).
    int64_t lb = std::lower_bound(cd.dict.begin(), cd.dict.end(), c.sval) - cd.dict.begin();
    bool present = lb < (int64_t)cd.dict.size() && cd.dict[lb] == c.sval;
    int64_t n = (int64_t)cd.dict.size();
    switch (c.op) {
      case CmpOp::Eq:
        if (!present) p.always_false = true;
        else p.preds.push_back({col, CmpOp::Eq, lb});
        break;
      case CmpOp::Ne:
        if (present) p.preds.push_back({col, CmpOp::Ne, lb});
        break;  // absent literal: always true
      case CmpOp::Lt:  // code < lb
        if (lb == 0) p.always_false = true;
        else if (lb < n) p.preds.push_back({col, CmpOp::Lt, lb});
        break;
      case CmpOp::Le:  // code < lb + present
        if (lb + present == 0) p.always_false = true;
        else if (lb + present < n) p.preds.push_back({col, CmpOp::Lt, lb + present});
        break;
      case CmpOp::Gt:  // code >= lb + present
        if (lb + present >= n) p.always_false = true;
        else if (lb + present > 0) p.preds.push_back({col, CmpOp::Ge, lb + present});
        break;
      case CmpOp::Ge:  // code >= lb
        if (lb >= n) p.always_false = true;
        else if (lb > 0) p.preds.push_back({col, CmpOp::Ge, lb});
        break;
    }
  }
  auto rank = [](CmpOp o) { return o == CmpOp::Eq ? 0 : o == CmpOp::Ne ? 2 : 1; };
  std::stable_sort(p.preds.begin(), p.preds.end(),
                   [&](const Pred& a, const Pred& b) { return rank(a.op) < rank(b.op); });

  // Column pruning.
  auto need = [&](int c) {
    if (c >= 0 && std::find(p.scan_cols.begin(), p.scan_cols.end(), c) == p.scan_cols.end())
      p.scan_cols.push_back(c);
  };
  for (auto& pr : p.preds) need(pr.col);
  need(p.group_col);
  for (auto& a : p.aggs) need(a.col);
  if (p.scan_cols.empty()) p.scan_cols.push_back(0);  // COUNT(*): need a row source
  return p;
}

inline std::string explain(const Plan& p, const Table& t) {
  std::ostringstream o;
  if (p.always_false) o << "ConstantFalse (dictionary proves no row can match)\n";
  o << "Aggregate";
  if (p.group_col >= 0) o << " group by " << t.cols[p.group_col].name;
  o << " [";
  for (size_t i = 0; i < p.aggs.size(); i++) {
    if (i) o << ", ";
    o << func_str(p.aggs[i].func) << "(" << (p.aggs[i].col < 0 ? "*" : t.cols[p.aggs[i].col].name) << ")";
  }
  o << "]\n  Filter (vectorized, 1024-row batches)";
  for (auto& pr : p.preds) o << "\n    " << t.cols[pr.col].name << " " << op_str(pr.op) << " " << pr.val;
  o << "\n    Scan cols=[";
  for (size_t i = 0; i < p.scan_cols.size(); i++) o << (i ? ", " : "") << t.cols[p.scan_cols[i]].name;
  o << "] zone-map pushdown on " << p.preds.size() << " predicate(s)\n";
  return o.str();
}

}  // namespace cq
