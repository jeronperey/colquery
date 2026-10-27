#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>

#include "baseline.h"
#include "exec.h"
#include "writer.h"

using namespace cq;
using Clock = std::chrono::steady_clock;

namespace {

double ms_since(Clock::time_point t0) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

uint64_t splitmix(uint64_t& s) {
  s += 0x9E3779B97F4A7C15ull;
  uint64_t z = s;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

size_t digits(int64_t v) {
  size_t d = 1;
  while (v >= 10) v /= 10, d++;
  return d;
}

// ---- gen: synthetic event table (ts, country, status, amount) -------------
// ts is monotonic (clustered -> zone maps prune range scans), country is a
// skewed low-cardinality string (dictionary), status has long runs (RLE),
// amount is uniform noise (frame-of-reference bit narrowing only).
int cmd_gen(uint64_t rows, const std::string& out, const std::string& csv_path) {
  static const char* kCountries[50] = {
      "US", "IN", "BR", "DE", "GB", "FR", "JP", "CA", "MX", "AU", "ES", "IT", "KR", "NL", "RU", "TR", "ID",
      "PL", "SE", "AR", "ZA", "EG", "NG", "SA", "AE", "CH", "NO", "DK", "FI", "IE", "PT", "GR", "CZ", "RO",
      "HU", "AT", "BE", "IL", "SG", "MY", "TH", "VN", "PH", "PK", "BD", "UA", "CL", "CO", "PE", "NZ"};
  std::vector<std::string> sorted(kCountries, kCountries + 50);
  std::sort(sorted.begin(), sorted.end());
  int64_t code_of[50];
  for (int i = 0; i < 50; i++)
    code_of[i] = std::lower_bound(sorted.begin(), sorted.end(), std::string(kCountries[i])) - sorted.begin();

  std::vector<Column> schema(4);
  schema[0] = {"ts", ColType::Int, {}, {}};
  schema[1] = {"country", ColType::Str, sorted, {}};
  schema[2] = {"status", ColType::Int, {}, {}};
  schema[3] = {"amount", ColType::Int, {}, {}};
  Writer w(out, schema);

  FILE* csv = csv_path.empty() ? nullptr : fopen(csv_path.c_str(), "wb");
  uint64_t source_bytes = 0;
  std::string line = "ts,country,status,amount\n";
  source_bytes += line.size();
  if (csv) fwrite(line.data(), 1, line.size(), csv);

  uint64_t rng = 42;
  int64_t ts = 1700000000000ll, status = 0;
  std::string csvbuf;
  auto t0 = Clock::now();
  for (uint64_t done = 0; done < rows;) {
    size_t n = (size_t)std::min<uint64_t>(kBlockRows, rows - done);
    std::vector<std::vector<int64_t>> blk(4, std::vector<int64_t>(n));
    for (size_t i = 0; i < n; i++) {
      uint64_t r = splitmix(rng);
      ts += 1 + (int64_t)(r % 20);
      double u = (double)(splitmix(rng) >> 11) * (1.0 / 9007199254740992.0);
      int ci = std::min(49, (int)(-std::log(1.0 - u) * 8.0));
      if ((r >> 40 & 63) == 0) status = (int64_t)(r >> 48) % 5;
      int64_t amount = 1 + (int64_t)((r >> 8) % 10000);
      blk[0][i] = ts;
      blk[1][i] = code_of[ci];
      blk[2][i] = status;
      blk[3][i] = amount;
      source_bytes += digits(ts) + strlen(kCountries[ci]) + digits(status) + digits(amount) + 4;  // 3 commas + \n
      if (csv) {
        csvbuf += std::to_string(ts) + "," + kCountries[ci] + "," + std::to_string(status) + "," +
                  std::to_string(amount) + "\n";
        if (csvbuf.size() > (1 << 20)) fwrite(csvbuf.data(), 1, csvbuf.size(), csv), csvbuf.clear();
      }
    }
    w.append_block(blk);
    done += n;
  }
  if (csv) {
    fwrite(csvbuf.data(), 1, csvbuf.size(), csv);
    fclose(csv);
  }
  w.finish(source_bytes);
  printf("generated %llu rows in %.1fs -> %s\n", (unsigned long long)rows, ms_since(t0) / 1000, out.c_str());
  return 0;
}

// ---- import: CSV with header -> CQF ----------------------------------------
std::vector<std::string> split_csv(const std::string& l) {
  std::vector<std::string> out;
  size_t s = 0;
  for (;;) {
    size_t e = l.find(',', s);
    out.push_back(l.substr(s, e == std::string::npos ? e : e - s));
    if (e == std::string::npos) return out;
    s = e + 1;
  }
}

int cmd_import(const std::string& in, const std::string& out) {
  std::ifstream f(in);
  if (!f) throw std::runtime_error("cannot open " + in);
  std::string line;
  std::getline(f, line);
  auto names = split_csv(line);
  size_t nc = names.size();
  if (nc > kMaxCols) throw std::runtime_error("too many columns");
  std::vector<std::vector<std::string>> cells(nc);
  uint64_t source_bytes = line.size() + 1;
  while (std::getline(f, line)) {
    if (line.empty()) continue;
    source_bytes += line.size() + 1;
    auto r = split_csv(line);
    if (r.size() != nc) throw std::runtime_error("ragged row: " + line);
    for (size_t c = 0; c < nc; c++) cells[c].push_back(std::move(r[c]));
  }
  size_t n = cells[0].size();
  std::vector<Column> schema(nc);
  std::vector<std::vector<int64_t>> vals(nc, std::vector<int64_t>(n));
  for (size_t c = 0; c < nc; c++) {
    schema[c].name = names[c];
    bool is_int = true;
    for (auto& s : cells[c]) {
      char* end;
      errno = 0;
      strtoll(s.c_str(), &end, 10);
      if (s.empty() || *end || errno) {
        is_int = false;
        break;
      }
    }
    if (is_int) {
      for (size_t i = 0; i < n; i++) vals[c][i] = strtoll(cells[c][i].c_str(), nullptr, 10);
    } else {
      schema[c].type = ColType::Str;
      schema[c].dict = cells[c];
      std::sort(schema[c].dict.begin(), schema[c].dict.end());
      schema[c].dict.erase(std::unique(schema[c].dict.begin(), schema[c].dict.end()), schema[c].dict.end());
      for (size_t i = 0; i < n; i++)
        vals[c][i] = std::lower_bound(schema[c].dict.begin(), schema[c].dict.end(), cells[c][i]) - schema[c].dict.begin();
    }
  }
  Writer w(out, schema);
  for (size_t s = 0; s < n; s += kBlockRows) {
    size_t e = std::min(n, s + kBlockRows);
    std::vector<std::vector<int64_t>> blk(nc);
    for (size_t c = 0; c < nc; c++) blk[c].assign(vals[c].begin() + s, vals[c].begin() + e);
    w.append_block(blk);
  }
  w.finish(source_bytes);
  printf("imported %zu rows, %zu columns -> %s\n", n, nc, out.c_str());
  return 0;
}

int cmd_info(const std::string& file) {
  Table t(file);
  printf("%llu rows, %zu blocks of %zu rows, file %.1f MB\n", (unsigned long long)t.nrows, t.nblocks, kBlockRows,
         t.size / 1e6);
  for (auto& c : t.cols) {
    size_t bytes = 0, rle = 0;
    for (auto& b : c.blocks) bytes += b.bytes, rle += b.enc == (uint8_t)Enc::Rle;
    printf("  %-10s %-6s %9.2f MB  RLE blocks %zu/%zu%s\n", c.name.c_str(), c.type == ColType::Str ? "string" : "int",
           bytes / 1e6, rle, c.blocks.size(),
           c.type == ColType::Str ? ("  dict=" + std::to_string(c.dict.size())).c_str() : "");
  }
  if (t.source_bytes)
    printf("raw CSV equivalent: %.1f MB -> compression ratio %.2fx\n", t.source_bytes / 1e6,
           (double)t.source_bytes / (double)t.size);
  return 0;
}

// ---- query -----------------------------------------------------------------
int cmd_query(const std::string& file, const std::string& sql, const std::string& engine) {
  Table t(file);
  Query q = parse_sql(sql);
  Plan p = make_plan(q, t);
  if (q.explain) {
    printf("%s", explain(p, t).c_str());
    return 0;
  }
  auto t0 = Clock::now();
  if (engine == "row") {
    RowStore rs = load_rows(t);
    t0 = Clock::now();
    Result r = run_row_at_a_time(t, rs, p);
    printf("%s-- row-at-a-time, %.1f ms\n", r.to_string().c_str(), ms_since(t0));
  } else {
    ScanStats st;
    Result r = run_vectorized(t, p, engine != "nozm", &st);
    printf("%s-- vectorized%s, %.1f ms, blocks scanned %zu / skipped %zu of %zu\n", r.to_string().c_str(),
           engine == "nozm" ? " (no zone maps)" : "", ms_since(t0), st.blocks_scanned, st.blocks_skipped,
           st.blocks_total);
  }
  return 0;
}

// ---- verify: independent brute force vs both engines ----------------------
bool eval_op(CmpOp op, int c) {
  switch (op) {
    case CmpOp::Eq: return c == 0;
    case CmpOp::Ne: return c != 0;
    case CmpOp::Lt: return c < 0;
    case CmpOp::Le: return c <= 0;
    case CmpOp::Gt: return c > 0;
    case CmpOp::Ge: return c >= 0;
  }
  return false;
}

// Evaluates WHERE directly on decoded values (strings compared as strings), so
// it does not depend on the planner's dictionary rewrite or on zone maps.
Result naive(const Table& t, const RowStore& rs, const Query& q, const Plan& p) {
  AggState st(p.aggs.size());
  std::map<int64_t, uint32_t> groups;
  if (p.group_col < 0) st.add_group(0);
  for (size_t i = 0; i < rs.n; i++) {
    const int64_t* r = &rs.data[i * rs.nc];
    bool ok = true;
    for (auto& c : q.where) {
      int ci = t.col_index(c.col);
      int cmp = t.cols[ci].type == ColType::Str ? t.cols[ci].dict[r[ci]].compare(c.sval)
                                                  : (r[ci] < c.ival ? -1 : r[ci] > c.ival ? 1 : 0);
      ok = ok && eval_op(c.op, cmp);
    }
    if (!ok) continue;
    uint32_t g = 0;
    if (p.group_col >= 0) {
      auto it = groups.find(r[p.group_col]);
      if (it == groups.end()) it = groups.emplace(r[p.group_col], st.add_group(r[p.group_col])).first;
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
  return finalize(t, p, st);
}

int cmd_verify(const std::string& file) {
  Table t(file);
  RowStore rs = load_rows(t);
  int64_t lo = INT64_MAX, hi = INT64_MIN;
  for (auto& b : t.cols[0].blocks) lo = std::min(lo, b.min), hi = std::max(hi, b.max);
  auto at = [&](double f) { return std::to_string(lo + (int64_t)((hi - lo) * f)); };
  std::vector<std::string> qs = {
      "SELECT COUNT(*) FROM t",
      "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) FROM t",
      "SELECT country, COUNT(*), SUM(amount) FROM t GROUP BY country",
      "SELECT status, AVG(amount), MAX(ts) FROM t GROUP BY status",
      "SELECT status, COUNT(*) FROM t WHERE amount > 9000 GROUP BY status",
      "SELECT COUNT(*), SUM(amount) FROM t WHERE ts >= " + at(0.4) + " AND ts < " + at(0.41),
      "SELECT COUNT(*) FROM t WHERE ts < " + at(0.0),
      "SELECT COUNT(*) FROM t WHERE ts <= " + at(0.0),
      "SELECT COUNT(*) FROM t WHERE ts > " + at(1.0),
      "SELECT COUNT(*) FROM t WHERE ts >= " + at(1.0),
      "SELECT COUNT(*), SUM(amount) FROM t WHERE ts != " + at(0.5),
      "SELECT COUNT(*) FROM t WHERE ts > -5",
      "SELECT country, MAX(amount) FROM t WHERE country = 'US' GROUP BY country",
      "SELECT COUNT(*) FROM t WHERE country = 'ZZ'",
      "SELECT COUNT(*) FROM t WHERE country != 'ZZ'",
      "SELECT COUNT(*) FROM t WHERE country < 'AA'",
      "SELECT COUNT(*) FROM t WHERE country < 'IN'",
      "SELECT COUNT(*) FROM t WHERE country <= 'IN'",
      "SELECT COUNT(*) FROM t WHERE country > 'IN'",
      "SELECT COUNT(*) FROM t WHERE country >= 'IN'",
      "SELECT COUNT(*) FROM t WHERE country > 'M'",
      "SELECT COUNT(*) FROM t WHERE country >= 'ZZZ'",
      "SELECT COUNT(*) FROM t WHERE country <= 'ZZZ'",
      "SELECT country, SUM(amount) FROM t WHERE ts > " + at(0.9) + " AND country = 'US' AND amount < 5000 GROUP BY country",
      "SELECT status, COUNT(*), SUM(amount) FROM t WHERE status = 3 AND amount >= 100 GROUP BY status",
      "select count(*) from t where status <> 0 and country != 'US';",
      "SELECT amount, COUNT(*) FROM t WHERE amount < 20 GROUP BY amount",
  };
  int bad = 0;
  for (auto& sql : qs) {
    Query q = parse_sql(sql);
    Plan p = make_plan(q, t);
    Result ref = naive(t, rs, q, p);
    Result a = run_vectorized(t, p, true), b = run_vectorized(t, p, false), c = run_row_at_a_time(t, rs, p);
    bool ok = a == ref && b == ref && c == ref;
    if (!ok) {
      bad++;
      printf("FAIL: %s\n--- expected\n%s--- vectorized\n%s--- row\n%s", sql.c_str(), ref.to_string().c_str(),
             a.to_string().c_str(), c.to_string().c_str());
    }
  }
  printf("%zu/%zu queries match the brute-force reference across 3 engines\n", qs.size() - bad, qs.size());
  return bad ? 1 : 0;
}

// ---- bench -----------------------------------------------------------------
template <class F>
double median_ms(int runs, F f) {
  std::vector<double> v;
  for (int i = 0; i < runs; i++) {
    auto t0 = Clock::now();
    f();
    v.push_back(ms_since(t0));
  }
  std::sort(v.begin(), v.end());
  return v[v.size() / 2];
}

int cmd_bench(const std::string& file, bool skip_baseline) {
  Table t(file);
  for (const char* c : {"ts", "country", "status", "amount"})
    if (t.col_index(c) < 0) throw std::runtime_error("bench expects the `gen` schema");
  printf("table: %llu rows, %zu blocks, %.1f MB on disk", (unsigned long long)t.nrows, t.nblocks, t.size / 1e6);
  if (t.source_bytes)
    printf(" (raw CSV %.1f MB, %.2fx smaller)", t.source_bytes / 1e6, (double)t.source_bytes / (double)t.size);
  printf("\n");

  RowStore rs;
  if (!skip_baseline) {
    auto t0 = Clock::now();
    rs = load_rows(t);
    printf("row store loaded for baseline: %.1f MB in %.1fs\n", rs.data.size() * 8 / 1e6, ms_since(t0) / 1000);
  }
  int64_t lo = INT64_MAX, hi = INT64_MIN;
  for (auto& b : t.cols[0].blocks) lo = std::min(lo, b.min), hi = std::max(hi, b.max);
  auto at = [&](double f) { return std::to_string(lo + (int64_t)((double)(hi - lo) * f)); };

  struct BQ {
    const char* name;
    std::string sql;
  };
  std::vector<BQ> qs = {
      {"full-scan agg", "SELECT COUNT(*), SUM(amount), AVG(amount) FROM t"},
      {"group by string", "SELECT country, SUM(amount), COUNT(*) FROM t GROUP BY country"},
      {"filter+group", "SELECT status, AVG(amount) FROM t WHERE amount > 9000 GROUP BY status"},
      {"1% ts range", "SELECT COUNT(*), SUM(amount) FROM t WHERE ts >= " + at(0.40) + " AND ts < " + at(0.41)},
      {"10% ts + country", "SELECT country, MAX(amount) FROM t WHERE ts >= " + at(0.90) + " AND country = 'US' GROUP BY country"},
  };
  printf("\n%-18s %10s %10s %10s %9s %9s %9s\n", "query", "vec+zm ms", "vec ms", "row ms", "vs row", "zm skip", "Mrows/s");
  double geo = 0;
  int ng = 0;
  bool all_ok = true;
  for (auto& bq : qs) {
    Query q = parse_sql(bq.sql);
    Plan p = make_plan(q, t);
    ScanStats st;
    Result rv, rn, rr;
    double tz = median_ms(5, [&] { rv = run_vectorized(t, p, true, &st); });
    double tn = median_ms(5, [&] { rn = run_vectorized(t, p, false); });
    double tr = 0;
    if (!skip_baseline) {
      tr = median_ms(3, [&] { rr = run_row_at_a_time(t, rs, p); });
      if (!(rv == rr && rn == rr)) {
        all_ok = false;
        printf("RESULT MISMATCH on %s\n", bq.name);
      }
    }
    double skip = 100.0 * (double)st.blocks_skipped / (double)st.blocks_total;
    printf("%-18s %10.1f %10.1f %10.1f %8.1fx %8.1f%% %9.0f\n", bq.name, tz, tn, tr, tr / tz, skip,
           (double)t.nrows / 1e3 / tz);
    if (!skip_baseline) geo += std::log(tr / tz), ng++;
  }
  if (ng) printf("\ngeomean speedup vs row-at-a-time: %.1fx   (results %s)\n", std::exp(geo / ng),
                 all_ok ? "identical across engines" : "MISMATCH");
  return all_ok ? 0 : 1;
}

void usage() {
  fprintf(stderr,
          "usage:\n"
          "  colquery gen <rows> <out.cq> [--csv file.csv]\n"
          "  colquery import <in.csv> <out.cq>\n"
          "  colquery info <file.cq>\n"
          "  colquery query <file.cq> \"<sql>\" [--engine vec|nozm|row]\n"
          "  colquery verify <file.cq>\n"
          "  colquery bench <file.cq> [--no-baseline]\n");
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::vector<std::string> a(argv + 1, argv + argc);
    auto flag = [&](const char* f, const std::string& def) {
      for (size_t i = 0; i + 1 < a.size(); i++)
        if (a[i] == f) return a[i + 1];
      return def;
    };
    if (a.size() >= 3 && a[0] == "gen") return cmd_gen(std::stoull(a[1]), a[2], flag("--csv", ""));
    if (a.size() >= 3 && a[0] == "import") return cmd_import(a[1], a[2]);
    if (a.size() >= 2 && a[0] == "info") return cmd_info(a[1]);
    if (a.size() >= 3 && a[0] == "query") return cmd_query(a[1], a[2], flag("--engine", "vec"));
    if (a.size() >= 2 && a[0] == "verify") return cmd_verify(a[1]);
    if (a.size() >= 2 && a[0] == "bench")
      return cmd_bench(a[1], std::find(a.begin(), a.end(), "--no-baseline") != a.end());
    usage();
    return 2;
  } catch (const std::exception& e) {
    fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
}
