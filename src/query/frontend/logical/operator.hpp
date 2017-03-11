#pragma once

#include <memory>
#include <vector>
#include <cstdio>

#include "database/graph_db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"
#include "query/context.hpp"
#include "fmt/format.h"

namespace query {

class ConsoleResultStream : public Loggable {
 public:
  ConsoleResultStream() : Loggable("ConsoleResultStream") {}

  void Header(const std::vector<std::string>&) { logger.info("header"); }

  void Result(std::vector<TypedValue>& values) {
    for (auto value : values) {
      logger.info("    result");
    }
  }

  void Summary(const std::map<std::string, TypedValue>&) {
    logger.info("summary");
  }
};

class Cursor {
 public:
  virtual bool pull(Frame&, SymbolTable&) = 0;
  virtual ~Cursor() {}
};

class LogicalOperator {
 public:
  auto children() { return children_; };
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor db) = 0;
  virtual std::shared_ptr<LogicalOperator> Compile(Context& ctx) = 0; 
  virtual void WriteHeader(ConsoleResultStream&) {}
  virtual std::vector<Symbol> OutputSymbols(SymbolTable& symbol_table) {
    return {};
  }
  virtual ~LogicalOperator() {}

 protected:
  std::vector<std::shared_ptr<LogicalOperator>> children_;
};

class ScanAll : public LogicalOperator {
 public:
  ScanAll(std::shared_ptr<NodePart> node_part) : node_part_(node_part) {}

 private:
  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(ScanAll& self, GraphDbAccessor db)
        : self_(self),
          db_(db),
          vertices_(db.vertices()),
          vertices_it_(vertices_.begin()) {}

    bool pull(Frame& frame, SymbolTable& symbol_table) override {
      while (vertices_it_ != vertices_.end()) {
        auto vertex = *vertices_it_++;
        if (evaluate(frame, symbol_table, vertex)) {
          return true;
        }
      }
      return false;
    }

   private:
    ScanAll& self_;
    GraphDbAccessor db_;
    decltype(db_.vertices()) vertices_;
    decltype(vertices_.begin()) vertices_it_;

    bool evaluate(Frame& frame, SymbolTable& symbol_table,
                  VertexAccessor& vertex) {
      auto node_part = self_.node_part_;
      for (auto label : node_part->labels_) {
        if (!vertex.has_label(label)) return false;
      }
      frame[symbol_table[node_part->identifier_].position_] = vertex;
      return true;
    }
  };

  PlanCompiler compiler;

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor db) override {
    return std::make_unique<Cursor>(ScanAllCursor(*this, db));
  }

  std::shared_ptr<LogicalOperator> Compile(Context& context) override {
    int compilation_id = context.NewCompilationId(); 
    std::string factory_method = fmt::format("CreateLogicalOperator{0}", compilation_id);
    std::string code = fmt::format(
        R"##(
            #include <memory>
            #include "query/frontend/logical/operator.hpp"
            #include "query/frontend/ast/ast.hpp"

            std::shared_ptr<LogicalOperator> {0}() {
              Ident ident(0); ident.identifier_ = "n";
              std::shared_ptr<NodePart> node_part = std::shared_ptr<NodePart>(new NodePart(1));
              node_part.identifier_ = ident; 
              return std::shared_ptr<LogicalOperator>(new ScanAll(node_part));
            }
        )##", factory_method);
    std::string cpp_name = std::tmpnam(nullptr);
    std::FILE* tmpf = std::fopen(cpp_name, "w");
    std::fputs(code, tmpf);
    std::fflush(tmpf);
    std::string so_name = std::tmpnam(nullptr);
    plan_compiler.compile(cpp_name, so_name);
    std::fclose(tmpf);
    void *so = dlopen(so_name.c_str(), RTLD_NOW);
    auto factory = reinterpret_cast<std::shared_ptr<LogicalOperator>(*)()>(dlsym(so, factory_method));
    return factory();
  }

 private:
  friend class ScanAll::ScanAllCursor;
  std::shared_ptr<NodePart> node_part_;
};

class Produce : public LogicalOperator {
 public:
  Produce(std::shared_ptr<LogicalOperator> input,
          std::vector<std::shared_ptr<NamedExpr>> exprs)
      : input_(input), exprs_(exprs) {
    children_.emplace_back(input);
  }

  void WriteHeader(ConsoleResultStream& stream) override {
    // TODO: write real result
    stream.Header({"n"});
  }

  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor db) override {
    return std::make_unique<Cursor>(ProduceCursor(*this, db));
  }

  std::vector<Symbol> OutputSymbols(SymbolTable& symbol_table) override {
    std::vector<Symbol> result(exprs_.size());
    for (auto named_expr : exprs_) {
        result.emplace_back(symbol_table[*named_expr->ident_]);
    }
    return result;
}

 private:
  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(Produce& self, GraphDbAccessor db)
        : self_(self), self_cursor_(self_.MakeCursor(db)) {}
    bool pull(Frame& frame, SymbolTable& symbol_table) override {
      if (self_cursor_->pull(frame, symbol_table)) {
        for (auto expr : self_.exprs_) {
          expr->Evaluate(frame, symbol_table);
        }
        return true;
      }
      return false;
    }

   private:
    Produce& self_;
    std::unique_ptr<Cursor> self_cursor_;
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor) override {
    return std::unique_ptr<Cursor>(new ProduceCursor(*this));
  }

 private:
  std::shared_ptr<LogicalOperator> input_;
  std::vector<std::shared_ptr<NamedExpr>> exprs_;
};
}
