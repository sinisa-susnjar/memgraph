//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#pragma once

#include <string>
#include <vector>
#include <cstdint>

using std::vector;


/**
 * Data structures used by the compiler. Used as support
 * (or full replacement) for the AST in the later phases
 * of compilation (semantic analysis and code generation).
 */
class DataStructures {

public:

  /**
   *  Following are accessors for various NamedElements,
   *  same functionality but different API.
   *
   *  The Get<>Index functions look for a NamedElement
   *  with the given name and return it's index if found,
   *  otherwise they create and insert a new NamedElement
   *  and return it's index.
   */
  class NamedElement {
  public:
    NamedElement(const std::string &name) : name_(name) {}
    const std::string name_;
    int32_t code_gen_id_{-1};
  };

  auto GetVariableIndex(const std::string &name) {
    return GetNamedElement(variables_, name);
  }

  const auto &Variables() const { return variables_; }

  auto GetPropertyIndex(const std::string &name) {
    return GetNamedElement(properties_, name);
  }

  const auto &Properties() const { return properties_; }

  auto GetLabelIndex(const std::string &name) {
    return GetNamedElement(labels_, name);
  }

  const auto &Labels() const { return labels_; }

  auto GetEdgeTypeIndex(const std::string &name) {
    return GetNamedElement(edge_types_, name);
  }

  const auto &EdgeTypes() const { return edge_types_; }


  /**
   * Following are the expression data structures.
   */

  enum class ExpressionOperand {
    Variable,
    Expression,
    Property,
    Label,
    EdgeType
    // TODO add all possible expression operands
  };

  enum class ExpressionOp {
    Addition,
    Subtraction
    // TODO add all expression ops
  };

  // TODO we have two template recursions here for the same thing
  // can this be more elegant?
  class Expression {
  public:
    Expression(ExpressionOp op) : op_(op) {}

    void AddOperands(ExpressionOperand operand_type,
                     int32_t operand_index) {
      operands_.emplace_back(operand_type, operand_index);
    }

    template <typename ... TArgs>
    void AddOperands(ExpressionOperand operand_type,
                     int32_t operand_index,
                     TArgs ... args) {
      operands_.emplace_back(operand_type, operand_index);
      AddOperands(args ...);
    }


    const ExpressionOp op_;
    vector<std::pair<ExpressionOperand, size_t>> operands_;
    int32_t code_gen_id_{-1};
  };

  auto AddExpression(ExpressionOp op) {
    expressions_.emplace_back(op);
    return expressions_.size() - 1;
  }

  template <typename ... TArgs>
  auto AddExpression(ExpressionOp op,
                     ExpressionOperand operand_type,
                     int32_t operand_index,
                     TArgs ... operands) {
    expressions_.emplace_back(op);
    expressions_.back().AddOperands(
        operand_type, operand_index, operands ...);

    return expressions_.size() - 1;
  }

  const auto &Expressions() const {
    return expressions_;
  }



  /**
   * The following functions are for pattern matching.
   */
   class Pattern {
     // TODO fill up with goodies
   };

  auto AddPattern(const Pattern &pattern) {
    patterns_.emplace_back(pattern);
    return patterns_.size() - 1;
  }

  const auto &Patterns() { return patterns_; }


  /**
   * The following stuff is for clauses. Note that for
   * each clause we keep it's type and a vector of
   * ints that point to clause elements. We assume
   * that each clause type can contain only one type of element
   * (though different for different clause types) and
   * we therefore don't need to track types on a per-element
   * basis. If this turns out to be untrue: refactor.
   */
   enum class ClauseType {
     Match,
     Merge,
     Create,
     Return
     // TODO add remaining clause types
   };
   class Clause {
   public:
     Clause(ClauseType type) : type_(type) {}

     const ClauseType type_;
     vector<int32_t> elements_;
   };

  /**
   * Unlike many other functions in this class,
   * AddClause returns the clause instead of it's
   * index, for convenience.
   *
   * @param type clause type
   * @return a reference to the created clause
   */
  Clause &AddClause(ClauseType type) {
    clauses_.emplace_back(type);
    return clauses_.back();
  }

  const auto &Clauses() { return clauses_; }

private:
  vector<NamedElement> variables_;
  vector<NamedElement> properties_;
  vector<NamedElement> labels_;
  vector<NamedElement> edge_types_;
  vector<Expression> expressions_;
  vector<Pattern> patterns_;
  vector<Clause> clauses_;

  /**
   * Helper function for getting a named element.
   *
   * @param collection The collection in which to search.
   * @param name Name to look for / add.
   * @return  Index of the NamedElement with the given name
   *    (found existing or added new).
   */
  int32_t GetNamedElement(vector<NamedElement> &collection,
                          const std::string &name) {

    for (int i = 0; i < collection.size(); ++i)
      if (collection[i].name_ == name)
        return i;

    collection.emplace_back(name);
    return collection.size() - 1;
  }
};
