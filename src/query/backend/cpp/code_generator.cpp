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

// TODO differentiate between nodes and edges?
const std::string kQueryVariableVarPrefix = "query_var_";

// prefixes for traversal variables
const std::string kTraversalVarPrefix = "traversal_";
const std::string kCartesianPrefix = "cartesian_";

using namespace query;

/*
 * Helper functions for emitting code.
 */

CodeGenerator &CodeGenerator::NL() {
  code_ += "\n";
  return *this;
}

CodeGenerator &CodeGenerator::Tab() {
  code_ += "\t";
  return *this;
}

CodeGenerator &CodeGenerator::Comm(const std::string &comment) {
  code_ += "// ";
  code_ += comment;
  return *this;
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

//  code_ += "\n// traversal\n";
  NL().Comm("traversal").NL();

  int current_traversal = 0;
  // TODO make a helper function for iterating over MATCH clauses and such
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
//  code_ += "\n// cartesian of all the traversals\n";
  NL().Comm("cartesian of all the traversals").NL();
  code_ += "auto " + kCartesianPrefix + "0 = Cartesian(";
  for (int i = 0; i < current_traversal; ++i) {
    if(i > 0) code_ += ", ";
    code_ += kTraversalVarPrefix + std::to_string(i);
  }
  code_ += ");\n";

}

void query::CodeGenerator::GeneratePatternTraversal(
    const DataStructures::Pattern &pattern, const int variable_index) {

  code_ += "auto " + kTraversalVarPrefix + std::to_string(variable_index);
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

void query::CodeGenerator::GenerateReturn() {
  // TODO for now only MATCH ... RETURN is supported
  code_ += "\n// return statement\n";

  // generate the basic visitor structure
  code_ += kCartesianPrefix + "0.Visit([](Paths &p) {\n";

  // TODO generate all the variables
  int path_counter = 0;
  code_ += "\t// variables defined in the query\n";
  for (auto &clause_ptr : data_structures_.clauses()) {
    if (clause_ptr->type_ == DataStructures::Clause::Type::MATCH) {
      DataStructures::Match &match = clause_ptr->As<DataStructures::Match>();
      for (auto pattern_ind : match.patterns_) {
        auto &pattern = data_structures_.patterns()[pattern_ind];

        auto first_node_var = pattern.nodes_[0].variable_;
        if (first_node_var != -1) {
          code_ += "\tauto ";
          code_ += kQueryVariableVarPrefix + std::to_string(first_node_var);
          code_ += " = p[" + std::to_string(path_counter) + "]";
          code_ += ".Vertices()[0];";
          code_ += "\t// " + data_structures_.variables()[first_node_var];
          code_ += "\n";
        }

        for (int relationship_ind = 0; relationship_ind < pattern.relationships_.size(); ++relationship_ind) {
          // TODO
        }

        // we're done with a pattern, so we're done with a yielded path
        path_counter++;
      }
    }
  }
  // TODO generate all the expressions

  // TODO return streams out
  // TODO return_all
  for (auto &clause_ptr : data_structures_.clauses()) {
    if (clause_ptr->type_ == DataStructures::Clause::Type::RETURN) {
      DataStructures::Return &match = clause_ptr->As<DataStructures::Return>();
    }}


  code_ += "};";
};
