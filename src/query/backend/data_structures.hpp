//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <climits>
#include <unordered_map>
#include <memory>
#include <typeinfo>

#include "utils/assert.hpp"

using std::vector;


/**
 * Data structures used by the compiler. Used as support
 * (or full replacement) for the AST in the later phases
 * of compilation (semantic analysis and code generation).
 */
class DataStructures {

public:

  /**
   *  Following are accessors for various query elements
   *  that are identified with a string.
   *
   *  The Get<X>Index(std::string name)
   *  functions look for element of type <X> that has
   *  the given name.
   */

  auto GetVariableIndex(const std::string &name) {
    return GetNamedElement(variables_, name);
  }

  const auto &variables() const { return variables_; }

  auto GetPropertyIndex(const std::string &name) {
    return GetNamedElement(properties_, name);
  }

  const auto &properties() const { return properties_; }

  auto GetLabelIndex(const std::string &name) {
    return GetNamedElement(labels_, name);
  }

  const auto &labels() const { return labels_; }

  auto GetEdgeTypeIndex(const std::string &name) {
    return GetNamedElement(edge_types_, name);
  }

  const auto &edge_types() const { return edge_types_; }


  /**
   * Following are the expression data structures.
   */

  enum class ExpressionOperand {
    VARIABLE,
    EXPRESSION,
    PROPERTY,
    LABEL,
    EDGE_TYPE
    // TODO add all possible expression operands
  };

  enum class ExpressionOp {
    LOGICAL_OR,
    LOGICAL_XOR,
    LOGICAL_AND,
    LOGICAL_NOT,
    EQ,
    NE,
    LT,
    GT,
    LE,
    GE,
    ADDITION,
    SUBTRACTION,
    MULTIPLICATION,
    DIVISION,
    MODULO,
    UNARY_MINUS,
    UNARY_PLUS,
    PROPERTY_GETTER,
    LITERAL,
    PARAMETER
    // TODO add all expression ops
  };

  /**
   * An expression parsed from the query. Contains
   * an enum that defines which operation the expression
   * should perform, and a vector of operands. Each
   * operand is defined by it's type and it's index.
   */
  struct Expression {
    const ExpressionOp op_;
    vector<std::pair<ExpressionOperand, size_t>> operands_;
  };

  /**
   * @param op Expression operation.
   * @return  Index of the added expression.
   */
  auto AddExpression(ExpressionOp op) {
    expressions_.emplace_back(Expression{op});
    return expressions_.size() - 1;
  }

  auto &expressions() { return expressions_; }


  /**
   * The following functions are for pattern matching.
   */
  struct Node {
    // node name, -1 if the node is not named
    int32_t variable_{-1};
    vector<int32_t> labels_;
    // pairs of (property_index, expression_index)
    vector<std::pair<int32_t, int32_t>> properties_;
  };

  struct Relationship {
    // relationship name, -1 if not named
    int32_t variable_{-1};
    enum Direction { LEFT, RIGHT, BOTH };
    Direction direction = Direction::BOTH;
    vector<int32_t> types_;
    // pairs of (property_index, expression_index)
    vector<std::pair<int32_t, int32_t>> properties_;

    bool has_range_= false;
    int64_t lower_bound = 1LL;
    int64_t upper_bound = LLONG_MAX;
  };

  // TODO: Flor prefers this is called Pattern because it's a single
  // pattern, and a collection of patterns should be Patterns or so
  struct Pattern {
    int32_t variable_{-1};
    vector<Node> nodes_;
    vector<Relationship> relationships_;
  };

  /**
   * @param start_node Starting node of the pattern.
   * @return Index of the newly created pattern.
   */
  // TODO should we std::move start_node into the vector?
  auto AddPattern(const Node &start_node) {
    patterns_.emplace_back();
    patterns_.back().nodes_.emplace_back(start_node);
    return patterns_.size() - 1;
  }

  auto &patterns() { return patterns_; }
  const auto &patterns() const { return patterns_; }


  /**
   * Following is a type hierarchy for clauses. All
   * inherit clause and define their own members.
   */
   class Clause {
   public:

     enum class Type {
       MATCH,
       UNWIND,
       MERGE,
       CREATE,
       SET,
       DELETE,
       REMOVE,
       WITH,
       RETURN
       // TODO add all other ones
     };

     const Type type_;

     Clause(Type type) : type_(type) {}
     virtual ~Clause() {}

     /**
      * Returns a reference to this Clause, cast to the desired
      * derived type. Throws std::bad_cast if the conversion is
      * not allowed.
      *
      * @tparam TDerived The type to return.
      * @return A reference to this, cast to TDerived
      */
     template <typename TDerived>
     TDerived &As() {
       auto *r_val = dynamic_cast<TDerived*>(this);
       if (r_val == nullptr)
         // TODO consider a specialized exception here
         throw std::bad_cast();

       return *r_val;
     }
   };

  /**
   * Returns all the clauses that can then be dynamically
   * cast to their exact type (which is known from clause.type_
   */
  auto &clauses() { return clauses_; }
  const auto &clauses() const { return clauses_; }

  class Match : public Clause {
  public:
    // optional WHERE expression
    int32_t expression_{-1};
    // indices of patterns in this match
    vector<int32_t> patterns_;

    Match() : Clause(Type::MATCH) {}
  };

  /** Adds a match clause and turns it's index in the clauses vector */
  auto AddMatch() {
    clauses_.emplace_back(std::make_unique<Match>(Match()));
    return clauses_.size() - 1;
  }

  class Return : public Clause {
  public:
    // if the return clause contains '*' at it's start
    bool return_all_;
    // vector of pairs (expression, variable)
    // where variable is -1 if there is no AS
    vector<std::pair<int32_t, int32_t>> expressions_;

    Return(bool return_all) : Clause(Type::RETURN),
                              return_all_(return_all) {}
  };

  /** Adds a return clause and returns it's index in the clauses vector */
  auto AddReturn(bool return_all) {
    clauses_.emplace_back(std::make_unique<Return>(Return(return_all)));
    return clauses_.size() - 1;
  }

private:
  vector<std::string> variables_;
  vector<std::string> properties_;
  vector<std::string> labels_;
  vector<std::string> edge_types_;
  vector<Expression> expressions_;
  vector<Pattern> patterns_;
  vector<std::unique_ptr<Clause>> clauses_;

  /**
   * Helper function for getting a named element.
   *
   * @param collection The collection in which to search.
   * @param name Name to look for / add.
   * @return  Index of the NamedElement with the given name
   *    (found existing or added new).
   */
  int32_t GetNamedElement(vector<std::string> &collection,
                          const std::string &name) {

    for (int i = 0; i < collection.size(); ++i)
      if (collection[i] == name)
        return i;

    collection.emplace_back(name);
    return collection.size() - 1;
  }
};
