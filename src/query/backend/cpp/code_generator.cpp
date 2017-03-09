#include "query/backend/cpp/code_generator.hpp"

#include <string>
#include <vector>

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

// prefixes for generated variable names
const std::string kPropVarPrefix = "property_";
const std::string kLabelVarPrefix = "label_";
const std::string kEdgeTypeVarPrefix = "edge_type_";

// prefixes for traversal variables
const std::string kTraversalVarPrefix = "traversal_";
const std::string kCartesianPrefix = "cartesian_";


void query::CodeGenerator::Generate() {
  code_.clear();
  GenerateNamedStuff();
  GenerateTraversal();
}

void query::CodeGenerator::GenerateTraversal() {
  // make a traversal for every pattern in every match
  // TODO support patterns elsewhere

  code_ += "\n// traversal\n";

  int current_traversal = 0;
  for (auto &clause_ptr : data_structures_.clauses()) {
    if (clause_ptr->type_ == DataStructures::Clause::Type::MATCH) {
      DataStructures::Match &match = clause_ptr->As<DataStructures::Match>();
      for (auto pattern_ind : match.patterns_)
        GeneratePatternTraversal(
            data_structures_.patterns()[pattern_ind],
            current_traversal++);
    }
  }

  // generate the final cartesian for all the traversals
  code_ += "\n// cartesian of all the traversals\n";
  code_ += "auto " + kCartesianPrefix + "0 = Cartesian(";
  for (int i = 0; i < current_traversal; ++i) {
    if(i > 0) code_ += ", ";
    code_ += kTraversalVarPrefix + std::to_string(i);
  }

}

void query::CodeGenerator::GeneratePatternTraversal(
    const DataStructures::Pattern &pattern, const int variable_index) {

  code_ += kTraversalVarPrefix + std::to_string(variable_index);
  code_ += " = ";

  // TODO currently we start traversal only from the whole vertex set
  code_ += "Begin(" + kDbAccessorVar + ".vertices())";

  for (int relationship_ind = 0; relationship_ind < pattern.relationships_.size(); ++relationship_ind) {
    const DataStructures::Relationship &relationship = pattern.relationships_[relationship_ind];
    const DataStructures::Node &node = pattern.nodes_[relationship_ind + 1];
    code_ += "\n\t.";
    code_ += relationship.has_range_ ? "ExpandVar(" : "Expand(";
    code_ += "Expansion::Back";
    switch (relationship.direction) {
      case DataStructures::Relationship::LEFT:
        code_ += ", Direction::In";
        break;
      case DataStructures::Relationship::RIGHT:
        code_ += ", Direction::Out";
        break;
      case DataStructures::Relationship::BOTH:
        code_ += ", Direction::Both";
        break;
    }

    // TODO vertex and edge filtering comes here
    if (relationship.has_range_) {
      // currently filters are not supported
      code_ += ", {}, {}";
      code_ += ", " + std::to_string(relationship.lower_bound);
      code_ += ", " + std::to_string(relationship.upper_bound);
    }
    code_ += ")";
  }
  code_ += ";\n";
}

/**
 * Generates the code that gets property, label and edge types
 * values for their name used in the query.
 */
void query::CodeGenerator::GenerateNamedStuff() {
  auto add_named = [this](const std::string &prop_var_prefix,
                                     const auto &collection,
                                     const std::string &db_accessor_func) {

    code_ += "\n// " + db_accessor_func + " variables\n";
    for (int i = 0; i < collection.size(); ++i)
    code_ += prop_var_prefix + std::to_string(i)
                      + " = " + kDbAccessorVar + "." + db_accessor_func
                      + "(\"" + collection[i] + "\");\n";

  };

  if (data_structures_.properties().size() > 0)
    add_named(kPropVarPrefix, data_structures_.properties(), "property");
  if (data_structures_.labels().size() > 0)
    add_named(kLabelVarPrefix, data_structures_.labels(), "label");
  if (data_structures_.edge_types().size() > 0)
    add_named(kEdgeTypeVarPrefix, data_structures_.edge_types(), "edge_type");
}

