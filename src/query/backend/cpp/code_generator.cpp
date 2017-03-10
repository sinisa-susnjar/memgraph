#include "query/backend/cpp/code_generator.hpp"

#include <string>
#include <vector>
#include <tuple>

#include "query/backend/cpp/named_antlr_tokens.hpp"
#include "utils/assert.hpp"

namespace {
std::string kLogicalOr = "||";
// OpenCypher supports xor operator only for booleans, so we can use binary xor
// instead.
std::string kLogicalXor = "^";
std::string kLogicalAnd = "&&";
std::string kLogicalNot = "!";
std::string kEq = "=";
std::string kNe = "!=";
std::string kLt = "<";
std::string kGt = ">";
std::string kLe = "<=";
std::string kGe = ">=";
std::string kPlus = "+";
std::string kMinus = "-";
std::string kMult = "*";
std::string kDiv = "/";
std::string kMod = "%";
std::string kUnaryMinus = "-";
std::string kUnaryPlus = "";  // No need to generate anything.
}

// names of variables in the template
const std::string kDbAccessorVar = "db_accessor";
const std::string kParamsVar = "params";
const std::string kStreamVar = "stream";

// prefixes for generated variable names
const std::string kPropVarPrefix = "property_";
const std::string kLabelVarPrefix = "label_";
const std::string kEdgeTypeVarPrefix = "edge_type_";
const std::string kParamVarPrefix = "param_";

// prefixes for traversal variables
const std::string kPatternVarPrefix = "pattern_";
const std::string kNodeVarPrefix = "node_";
const std::string kRelationshipVarPrefix = "relationship_";
const std::string kFilterSuffix = "_filter";
const std::string kLocalVertexVar = "vertex";
const std::string kLocalEdgeVar = "edge";
const std::string kTraversalVarPrefix = "traversal_";
const std::string kCartesianPrefix = "cartesian_";

// prefixes for variables containing TypedValues
const std::string kQueryVariableVarPrefix = "query_var_";
const std::string kExpressionVarPrefix = "expression_";

// names of classes
const std::string kVertexClass = "VertexAccessor";
const std::string kEdgeClass = "EdgeAccessor";


using namespace query;

/*
 * Helper functions for emitting code.
 */

CodeGenerator &CodeGenerator::NL() {
  code_ += "\n";
  return *this;
}

CodeGenerator &CodeGenerator::NL(const std::string &s) {
  code_ += s;
  return NL();
}

CodeGenerator &CodeGenerator::Tab(int tabs) {
  for (int i = 0; i < tabs; ++i)
    code_ += "\t";
  return *this;
}

CodeGenerator &CodeGenerator::Comm() {
  code_ += "// ";
  return *this;
}

CodeGenerator &CodeGenerator::Comm(const std::string &comment) {
  return Comm().Emit(comment);
}

CodeGenerator &CodeGenerator::Emit(const std::string &s) {
  code_ += s;
  return *this;
}

CodeGenerator &CodeGenerator::Emit(const char *s) {
  return Emit(std::string(s));
}

void query::CodeGenerator::Generate() {
  code_.clear();
  GenerateNamedStuff();
  GenerateTraversal();
  GenerateReturn();
}

void query::CodeGenerator::GenerateTraversal() {
  // make a traversal for every pattern in every match
  // TODO support patterns elsewhere

  NL().Comm("traversal").NL();

  // iterate through all the patterns in all the matches
  int pattern_index = 0;
  for (auto &match : data_structures_.Matches())
    for (auto pattern_ind : match.get().patterns_) {
      const auto &pattern = data_structures_.patterns()[pattern_ind];

      // generate node filters
      int current_node = 0;
      for (const auto &node : pattern.nodes_)
        GenerateVertexFilter(pattern_index, current_node++, node);

      // generate edge filters
      int current_relationship = 0;
      for (const auto &relationship : pattern.relationships_)
        GenerateRelationshipFilter(pattern_index, current_relationship++, relationship);

      GeneratePatternTraversal(pattern_index++);
    }

  // generate the final cartesian for all the traversals
  Fmt("auto {}0 = Cartesian(", kCartesianPrefix);
  for (int i = 0; i < pattern_index; ++i) {
    if (i > 0) Emit(", ");
    Emit(kTraversalVarPrefix, i);
  }
  NL(");");
}

const std::string NodeFilterVarName(int pattern_ind, int node_ind) {
  return fmt::format(
      "{}{}_{}{}{}", kTraversalVarPrefix, pattern_ind,
      kNodeVarPrefix, node_ind, kFilterSuffix);
}

const std::string RelationshipFilterVarName(int pattern_ind, int relationship_ind) {
  return fmt::format(
      "{}{}_{}{}{}", kTraversalVarPrefix, pattern_ind,
      kRelationshipVarPrefix, relationship_ind, kFilterSuffix);
}

bool CodeGenerator::GenerateVertexFilter(
    int pattern_ind, int node_ind, const DataStructures::Node &node) {
  if (node.labels_.size() > 0 || node.properties_.size() > 0) {
    bool is_first = true;
    Fmt("auto {} = [](const {} &{})",
        NodeFilterVarName(pattern_ind, node_ind), kVertexClass, kLocalVertexVar);
    for (auto label : node.labels_) {
      NL().Tab();
      if (!is_first) {
        Tab().Emit("&& ");
      } else {
        Emit("return ");
        is_first = false;
      }
      Fmt("{}.has_label({}{})", kLocalVertexVar, kLabelVarPrefix, label);
    }

    // TODO generate expression based filters, if possible here

    Emit(";").NL().Emit("};").NL();
    return true;
  } else
    return false;
}

bool CodeGenerator::GenerateRelationshipFilter(
    int pattern_ind, int relationship_ind, const DataStructures::Relationship &relationship) {
  if (relationship.types_.size() > 0 || relationship.properties_.size() > 0) {
    bool is_first = true;
    Fmt("auto {} = [](const {} &{})",
        RelationshipFilterVarName(pattern_ind, relationship_ind), kEdgeClass, kLocalEdgeVar);
    for (auto edge_type : relationship.types_) {
      NL().Tab();
      if (!is_first) {
        Tab().Emit("&& ");
      } else {
        Emit("return ");
        is_first = false;
      }
      Fmt("{}.edge_type == {}{}", kLocalEdgeVar, kEdgeTypeVarPrefix, edge_type);
    }

    // TODO generate expression based filters, if possible here

    Emit(";").NL().Emit("};").NL();
    return true;
  } else
    return false;
}

void CodeGenerator::GeneratePatternTraversal(const int pattern_index) {
  const DataStructures::Pattern &pattern = data_structures_.patterns()[pattern_index];

  const DataStructures::Node &first_node = pattern.nodes_[0];
  Fmt("auto {}{} = Begin({}.vertices()",
      kTraversalVarPrefix, pattern_index, kDbAccessorVar);
  if (first_node.labels_.size() > 0 || first_node.properties_.size() > 0)
    Fmt(", {}", NodeFilterVarName(pattern_index, 0));
  Emit(")");

  for (int relationship_ind = 0; relationship_ind < pattern.relationships_.size(); ++relationship_ind) {
    const DataStructures::Relationship &relationship = pattern.relationships_[relationship_ind];
    const DataStructures::Node &node = pattern.nodes_[relationship_ind + 1];
    NL().Fmt("\t.{}(Expression::Back, ",
             relationship.has_range_ ? "ExpandVar" : "Expand");
    switch (relationship.direction_) {
      case DataStructures::Relationship::Direction::LEFT:
        Emit("Direction::In");
        break;
      case DataStructures::Relationship::Direction::RIGHT:
        Emit("Direction::Out");
        break;
      case DataStructures::Relationship::Direction::BOTH:
        Emit("Direction::Both");
        break;
    }

    // node filtering
    if (node.labels_.size() > 0 || node.properties_.size() > 0)
      Fmt(", {}", NodeFilterVarName(pattern_index, relationship_ind + 1));
    else
      Emit(", {}");

    // relationship filtering
    if (relationship.types_.size() > 0 || relationship.properties_.size() > 0)
      Fmt(", {}", RelationshipFilterVarName(pattern_index, relationship_ind));
    else
      Emit(", {}");

    if (relationship.has_range_)
      Fmt(", {}, {}", relationship.lower_bound, relationship.upper_bound);

    Emit(")");
  }
  Emit(";").NL();
}

/**
 * Generates the code that gets property, label and edge types
 * values for their name used in the query.
 */
void query::CodeGenerator::GenerateNamedStuff() {
  auto add_named = [this](const std::string &prop_var_prefix,
                          const auto &collection,
                          const std::string &collection_name,
                          const std::string &db_accessor_func) {

    for (int i = 0; i < collection.size(); ++i)
      Fmt("auto {}{} = {}.{}(\"{}\");", prop_var_prefix, i, collection_name,
          db_accessor_func, collection[i]).NL();
  };

  if (data_structures_.properties().size())
    add_named(kPropVarPrefix, data_structures_.properties(), kDbAccessorVar, "property");

  if (data_structures_.labels().size())
    add_named(kLabelVarPrefix, data_structures_.labels(), kDbAccessorVar, "label");

  if (data_structures_.edge_types().size())
    add_named(kEdgeTypeVarPrefix, data_structures_.edge_types(), kDbAccessorVar, "edge_type");

  if (data_structures_.params().size())
    add_named(kParamVarPrefix, data_structures_.params(), kParamsVar, "At");
}

void query::CodeGenerator::GenerateReturn() {
  // TODO for now only MATCH ... RETURN is supported
  NL().Comm("return statement").NL();

  // headers
  NL().Comm("headers");
  NL().Emit(kStreamVar, ".Header(std::vector{");
  for (auto &ret : data_structures_.Returns()) {
    for (const auto &return_element : ret.get().expressions_)
    NL().Tab().Fmt("\"{}\"", std::get<1>(return_element));
  }
  Emit("});").NL();

  // generate the basic visitor structure
  NL().Emit(kCartesianPrefix, "0.Visit([](Paths &p) {").NL();

  // TODO generate all the variables
  int path_counter = 0;
  Tab().Comm("variables defined in the query").NL();
  for (auto match : data_structures_.Matches())
    for (auto pattern_ind : match.get().patterns_) {
      auto &pattern = data_structures_.patterns()[pattern_ind];
      auto first_node_var = pattern.nodes_[0].variable_;
      if (first_node_var != -1) {
        Tab().Fmt("auto {}{} = p[{}].Vertices()[0];",
                  kQueryVariableVarPrefix, first_node_var, path_counter)
            .Tab().Comm()
            .Emit(data_structures_.variables()[first_node_var]).NL();
      }

      for (int relationship_ind = 0; relationship_ind < pattern.relationships_.size(); ++relationship_ind) {
        auto node_var = pattern.nodes_[relationship_ind + 1].variable_;
        if (node_var != -1) {
          Tab().Fmt("auto {}{} = p[{}].Vertices()[{}];",
                    kQueryVariableVarPrefix, node_var, path_counter, relationship_ind + 1)
              .Tab().Comm()
              .Emit(data_structures_.variables()[node_var]).NL();
        }
      }

      // we're done with a pattern, so we're done with a yielded path
      path_counter++;
    }

  NL().Tab().Comm("expressions defined in the query").NL();
  GenerateExpressions();

  NL().Tab().Comm("streaming out return statements").NL();
  for (auto &ret : data_structures_.Returns()) {
    // TODO write out header

    // TODO return_all

    // TODO write out results
    Tab().Emit(kStreamVar, ".Result(std::vector<TypedValue>{");
    for (auto expression : ret.get().expressions_)
      NL().Tab(2).Fmt("{}{},", kExpressionVarPrefix, std::get<0>(expression));
    NL().Tab().Emit("});").NL();

    // TODO write out meta
    NL().Comm("TODO write out metdata").NL();
  }

  Emit("};");
};

void CodeGenerator::GenerateExpressions() {
  for (int expression_ind = 0; expression_ind < data_structures_.expressions().size(); ++expression_ind) {
    const DataStructures::Expression expression = data_structures_.expressions()[expression_ind];
    Tab().Fmt("auto {}{} = ", kExpressionVarPrefix, expression_ind);
    switch (expression.op_) {
      case DataStructures::ExpressionOp::VARIABLE:
        Fmt("{}{}", kQueryVariableVarPrefix, expression.operands_[0].second);
        break;

      case DataStructures::ExpressionOp::PROPERTY_GETTER:
        Fmt("{}{}.PropsAt({}{})",
            kExpressionVarPrefix, expression.operands_[0].second,
            kPropVarPrefix, expression.operands_[1].second);
        break;

      case DataStructures::ExpressionOp::PARAMETER:
        Fmt("{}{}", kParamVarPrefix, expression.operands_[0].second);
        break;

      case DataStructures::ExpressionOp::LOGICAL_OR:
      case DataStructures::ExpressionOp::LOGICAL_XOR:
      case DataStructures::ExpressionOp::LOGICAL_AND:
      case DataStructures::ExpressionOp::LOGICAL_NOT:
      case DataStructures::ExpressionOp::EQ:
      case DataStructures::ExpressionOp::NE:
      case DataStructures::ExpressionOp::LT:
      case DataStructures::ExpressionOp::GT:
      case DataStructures::ExpressionOp::LE:
      case DataStructures::ExpressionOp::GE:
      case DataStructures::ExpressionOp::ADDITION:
      case DataStructures::ExpressionOp::SUBTRACTION:
      case DataStructures::ExpressionOp::MULTIPLICATION:
      case DataStructures::ExpressionOp::DIVISION:
      case DataStructures::ExpressionOp::MODULO:
      case DataStructures::ExpressionOp::UNARY_MINUS:
      case DataStructures::ExpressionOp::UNARY_PLUS:
      case DataStructures::ExpressionOp::LITERAL:
      permanent_fail("Not yet implemented");
    }

    Emit(";").NL();
  }
}
