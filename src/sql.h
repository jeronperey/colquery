// Tiny SQL front end:
//   [EXPLAIN] SELECT item[, item...] FROM t [WHERE col op lit [AND ...]] [GROUP BY col]
//   item := col | COUNT(*) | COUNT|SUM|MIN|MAX|AVG(col)     op := = != <> < <= > >=
#pragma once
#include <cctype>
#include <stdexcept>
#include <string>
#include <vector>

namespace cq {

enum class CmpOp { Eq, Ne, Lt, Le, Gt, Ge };
enum class Func { Count, Sum, Min, Max, Avg };

struct SelectItem {
  bool is_agg = false;
  Func func = Func::Count;
  bool star = false;
  std::string col;
};

struct Cond {
  std::string col;
  CmpOp op = CmpOp::Eq;
  bool is_str = false;
  int64_t ival = 0;
  std::string sval;
};

struct Query {
  bool explain = false;
  std::vector<SelectItem> items;
  std::string table;
  std::vector<Cond> where;
  std::string group_by;
};

namespace detail {

struct Token {
  enum Kind { Ident, Num, Str, Sym, End } kind;
  std::string s;
  int64_t v = 0;
};

inline std::string lower(std::string s) {
  for (auto& c : s) c = (char)tolower((unsigned char)c);
  return s;
}

inline std::vector<Token> lex(const std::string& q) {
  std::vector<Token> out;
  size_t i = 0;
  while (i < q.size()) {
    char c = q[i];
    if (isspace((unsigned char)c)) {
      i++;
    } else if (isalpha((unsigned char)c) || c == '_') {
      size_t j = i;
      while (j < q.size() && (isalnum((unsigned char)q[j]) || q[j] == '_')) j++;
      out.push_back({Token::Ident, q.substr(i, j - i), 0});
      i = j;
    } else if (isdigit((unsigned char)c)) {
      size_t j = i;
      while (j < q.size() && isdigit((unsigned char)q[j])) j++;
      out.push_back({Token::Num, q.substr(i, j - i), std::stoll(q.substr(i, j - i))});
      i = j;
    } else if (c == '\'') {
      size_t j = i + 1;
      std::string s;
      while (j < q.size() && q[j] != '\'') s += q[j++];
      if (j >= q.size()) throw std::runtime_error("unterminated string literal");
      out.push_back({Token::Str, s, 0});
      i = j + 1;
    } else {
      std::string two = q.substr(i, 2);
      if (two == "<=" || two == ">=" || two == "!=" || two == "<>") {
        out.push_back({Token::Sym, two, 0});
        i += 2;
      } else if (std::string("=<>(),*;-").find(c) != std::string::npos) {
        out.push_back({Token::Sym, std::string(1, c), 0});
        i++;
      } else {
        throw std::runtime_error(std::string("unexpected character '") + c + "'");
      }
    }
  }
  out.push_back({Token::End, "", 0});
  return out;
}

class Parser {
 public:
  explicit Parser(const std::string& q) : t_(lex(q)) {}

  Query parse() {
    Query q;
    if (kw("explain")) q.explain = true;
    expect_kw("select");
    do q.items.push_back(select_item());
    while (sym(","));
    expect_kw("from");
    q.table = ident("table name");
    if (kw("where")) {
      do q.where.push_back(cond());
      while (kw("and"));
    }
    if (kw("group")) {
      expect_kw("by");
      q.group_by = ident("column name");
    }
    sym(";");
    if (t_[i_].kind != Token::End) throw std::runtime_error("unexpected token '" + t_[i_].s + "'");
    return q;
  }

 private:
  bool kw(const char* w) {
    if (t_[i_].kind == Token::Ident && lower(t_[i_].s) == w) {
      i_++;
      return true;
    }
    return false;
  }
  void expect_kw(const char* w) {
    if (!kw(w)) throw std::runtime_error(std::string("expected ") + w);
  }
  bool sym(const char* s) {
    if (t_[i_].kind == Token::Sym && t_[i_].s == s) {
      i_++;
      return true;
    }
    return false;
  }
  std::string ident(const char* what) {
    if (t_[i_].kind != Token::Ident) throw std::runtime_error(std::string("expected ") + what);
    return t_[i_++].s;
  }

  SelectItem select_item() {
    SelectItem it;
    std::string name = ident("column or aggregate");
    if (sym("(")) {
      std::string f = lower(name);
      it.is_agg = true;
      if (f == "count") it.func = Func::Count;
      else if (f == "sum") it.func = Func::Sum;
      else if (f == "min") it.func = Func::Min;
      else if (f == "max") it.func = Func::Max;
      else if (f == "avg") it.func = Func::Avg;
      else throw std::runtime_error("unknown function " + name);
      if (sym("*")) {
        if (it.func != Func::Count) throw std::runtime_error("only COUNT accepts *");
        it.star = true;
      } else {
        it.col = ident("column name");
      }
      if (!sym(")")) throw std::runtime_error("expected )");
    } else {
      it.col = name;
    }
    return it;
  }

  Cond cond() {
    Cond c;
    c.col = ident("column name");
    if (sym("=")) c.op = CmpOp::Eq;
    else if (sym("!=") || sym("<>")) c.op = CmpOp::Ne;
    else if (sym("<=")) c.op = CmpOp::Le;
    else if (sym(">=")) c.op = CmpOp::Ge;
    else if (sym("<")) c.op = CmpOp::Lt;
    else if (sym(">")) c.op = CmpOp::Gt;
    else throw std::runtime_error("expected comparison operator");
    bool neg = sym("-");
    if (t_[i_].kind == Token::Num) {
      c.ival = neg ? -t_[i_].v : t_[i_].v;
      i_++;
    } else if (!neg && t_[i_].kind == Token::Str) {
      c.is_str = true;
      c.sval = t_[i_++].s;
    } else {
      throw std::runtime_error("expected literal");
    }
    return c;
  }

  std::vector<Token> t_;
  size_t i_ = 0;
};

}  // namespace detail

inline Query parse_sql(const std::string& q) { return detail::Parser(q).parse(); }

}  // namespace cq
