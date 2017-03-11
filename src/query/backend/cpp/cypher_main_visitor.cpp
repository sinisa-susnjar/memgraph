#include "query/backend/cpp/cypher_main_visitor.hpp"

#include <climits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "query/backend/cpp/named_antlr_tokens.hpp"
#include "utils/assert.hpp"
#include "utils/exceptions/basic_exception.hpp"

namespace backend {
namespace cpp {

using namespace query;

namespace {
// Map children tokens of antlr node to DataStructures::ExpressionOp enum.
std::vector<DataStructures::ExpressionOp> MapTokensToOperators(
    antlr4::ParserRuleContext *node,
    const std::unordered_map<size_t, DataStructures::ExpressionOp>
        token_to_operator) {
  std::vector<antlr4::tree::TerminalNode *> tokens;
  for (const auto &x : token_to_operator) {
    tokens.insert(tokens.end(), node->getTokens(x.first).begin(),
                  node->getTokens(x.first).end());
  }
  sort(tokens.begin(), tokens.end(), [](antlr4::tree::TerminalNode *a,
                                        antlr4::tree::TerminalNode *b) {
    return a->getSourceInterval().startsBeforeDisjoint(b->getSourceInterval());
  });
  std::vector<DataStructures::ExpressionOp> ops;
  for (auto *token : tokens) {
    auto it = token_to_operator.find(token->getSymbol()->getType());
    debug_assert(it != token_to_operator.end(),
                 "Wrong mapping sent to function.");
    ops.push_back(it->second);
  }
  return ops;
}
}

// This shouldn't be called SemanticException since this has nothing to do with
// semantics.
// TODO: Figure out what information to put in exception.
// Error reporting is tricky since we get stripped query and position of error
// in original query is not same as position of error in stripped query. Most
// correct approach would be to do semantic analysis with original query even
// for already hashed queries, but that has obvious performance issues. Other
// approach would be to report some of the semantic errors in runtime of the
// query and only report line numbers of semantic errors (not position in the
// line) if multiple line strings are not allowed by grammar. We could also
// print whole line that contains error instead of specifying line number.
class SemanticException : BasicException {
public:
  SemanticException() : BasicException("") {}
};

antlrcpp::Any
CypherMainVisitor::visitNodePattern(CypherParser::NodePatternContext *ctx) {
  auto node = DataStructures::Node();
  if (ctx->variable()) {
    auto variable = ctx->variable()->accept(this).as<std::pair<Operand, int>>();
    node.variable_ = variable.second;
  }
  // THIS COMMENT IS OBSOLETE SINCE THIS KIND OF CHECKS SHOULD BE DONE IN
  // SEMANTIC ANALYSIS (NOT YET IMPLEMENTED).
  // If variable is already declared, we cannot list properties or labels.
  // This is slightly incompatible with neo4j. In neo4j it is valid to write
  // MATCH (n {a: 5})--(n {b: 10}) which is equivalent to MATCH(n {a:5, b:
  // 10})--(n). Neo4j also allows MATCH (n) RETURN (n {x: 5}) which is
  // equivalent to MATCH (n) RETURN ({x: 5}).
  // TODO: The way in which we are storing nodes is not suitable for optional
  // match. For example: MATCH (n {a: 5}) OPTIONAL MATCH (n {b: 10}) RETURN
  // n.a, n.b. would not work. Think more about that case.
  if (ctx->nodeLabels()) {
    auto labels =
        ctx->nodeLabels()->accept(this).as<std::vector<std::string>>();
    for (const auto &label : labels) {
      node.labels_.push_back(ds_.GetLabelIndex(label));
    }
  }
  if (ctx->properties()) {
    auto properties =
        ctx->properties()
            ->accept(this)
            .as<std::unordered_map<std::string, std::pair<Operand, int>>>();
    for (const auto property : properties) {
      auto key = ds_.GetPropertyIndex(property.first);
      node.properties_.emplace_back(key, property.second.second);
    }
  }
  return node;
}

antlrcpp::Any
CypherMainVisitor::visitNodeLabels(CypherParser::NodeLabelsContext *ctx) {
  std::vector<std::string> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    labels.push_back(node_label->accept(this).as<std::string>());
  }
  return labels;
}

antlrcpp::Any
CypherMainVisitor::visitProperties(CypherParser::PropertiesContext *ctx) {
  if (!ctx->mapLiteral()) {
    // If child is not mapLiteral that means child is params. At the moment
    // memgraph doesn't support params.
    throw SemanticException();
  }
  return ctx->mapLiteral()->accept(this);
}

antlrcpp::Any
CypherMainVisitor::visitMapLiteral(CypherParser::MapLiteralContext *ctx) {
  std::unordered_map<std::string, std::string> map;
  for (int i = 0; i < (int)ctx->propertyKeyName().size(); ++i) {
    map[ctx->propertyKeyName()[i]->accept(this).as<std::string>()] =
        ctx->expression()[i]->accept(this).as<std::string>();
  }
  return map;
}

antlrcpp::Any
CypherMainVisitor::visitVariable(CypherParser::VariableContext *ctx) {
  auto text = ctx->symbolicName()->accept(this).as<std::string>();
  auto var = ds_.GetVariableIndex(text);
  return std::pair<Operand, int>(Operand::VARIABLE, var);
}

antlrcpp::Any
CypherMainVisitor::visitSymbolicName(CypherParser::SymbolicNameContext *ctx) {
  if (ctx->EscapedSymbolicName()) {
    // We don't allow at this point for variable to be EscapedSymbolicName
    // because we would have t ofigure out how escaping works since same
    // variable can be referenced in two ways: escaped and unescaped.
    throw SemanticException();
  }
  return std::string(ctx->getText());
}

antlrcpp::Any
CypherMainVisitor::visitPattern(CypherParser::PatternContext *ctx) {
  std::vector<int> patterns;
  for (auto *pattern_part : ctx->patternPart()) {
    patterns.push_back(pattern_part->accept(this).as<int>());
  }
  return patterns;
}

antlrcpp::Any
CypherMainVisitor::visitPatternPart(CypherParser::PatternPartContext *ctx) {
  auto pattern_id = ctx->anonymousPatternPart()->accept(this).as<int>();
  if (ctx->variable()) {
    auto variable = ctx->variable()->accept(this).as<std::pair<Operand, int>>();
    ds_.patterns()[pattern_id].variable_ = variable.second;
  }
  return pattern_id;
}

antlrcpp::Any CypherMainVisitor::visitPatternElement(
    CypherParser::PatternElementContext *ctx) {
  if (ctx->patternElement()) {
    return ctx->patternElement()->accept(this);
  }
  auto node = ctx->nodePattern()->accept(this).as<DataStructures::Node>();
  auto pattern = ds_.AddPattern(node);
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    auto element = pattern_element_chain->accept(this)
                       .as<std::pair<DataStructures::Node,
                                     DataStructures::Relationship>>();
    pattern.second.nodes_.push_back(element.first);
    pattern.second.relationships_.push_back(element.second);
  }
  return pattern.first;
}

antlrcpp::Any CypherMainVisitor::visitPatternElementChain(
    CypherParser::PatternElementChainContext *ctx) {
  return std::pair<std::string, std::string>(
      ctx->relationshipPattern()->accept(this).as<std::string>(),
      ctx->nodePattern()->accept(this).as<std::string>());
}

antlrcpp::Any CypherMainVisitor::visitRelationshipPattern(
    CypherParser::RelationshipPatternContext *ctx) {
  auto direction = [&]() {
    if (ctx->leftArrowHead() && !ctx->rightArrowHead()) {
      return DataStructures::Relationship::Direction::LEFT;
    } else if (!ctx->leftArrowHead() && ctx->rightArrowHead()) {
      return DataStructures::Relationship::Direction::RIGHT;
    } else {
      // <-[]-> and -[]- is the same thing as far as we understand openCypher
      // grammar.
      return DataStructures::Relationship::Direction::BOTH;
    }
  }();
  auto relationship = DataStructures::Relationship(direction);

  if (ctx->relationshipDetail()) {
    if (ctx->relationshipDetail()->variable()) {
      auto variable = ctx->relationshipDetail()
                          ->variable()
                          ->accept(this)
                          .as<std::pair<Operand, int>>();
      relationship.variable_ = variable.second;
    }
    if (ctx->relationshipDetail()->relationshipTypes()) {
      auto types = ctx->relationshipDetail()
                       ->relationshipTypes()
                       ->accept(this)
                       .as<std::vector<std::string>>();
      for (const auto type : types) {
        relationship.types_.push_back(ds_.GetEdgeTypeIndex(type));
      }
    }
    if (ctx->relationshipDetail()->properties()) {
      auto properties =
          ctx->relationshipDetail()
              ->properties()
              ->accept(this)
              .as<std::unordered_map<std::string, std::pair<Operand, int>>>();
      for (const auto property : properties) {
        auto key = ds_.GetPropertyIndex(property.first);
        relationship.properties_.emplace_back(key, property.second.second);
      }
      if (ctx->relationshipDetail()->rangeLiteral()) {
        relationship.has_range_ = true;
        auto range = ctx->relationshipDetail()
                         ->rangeLiteral()
                         ->accept(this)
                         .as<std::pair<int64_t, int64_t>>();
        relationship.lower_bound = range.first;
        relationship.upper_bound = range.second;
      }
    }
  }
  return relationship;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipDetail(
    CypherParser::RelationshipDetailContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipTypes(
    CypherParser::RelationshipTypesContext *ctx) {
  std::vector<std::string> types;
  for (auto *label : ctx->relTypeName()) {
    types.push_back(label->accept(this).as<std::string>());
  }
  return types;
}

antlrcpp::Any
CypherMainVisitor::visitRangeLiteral(CypherParser::RangeLiteralContext *ctx) {
  if (ctx->integerLiteral().size() == 0U) {
    // -[*]-
    return std::pair<long long, long long>(1LL, LLONG_MAX);
  } else if (ctx->integerLiteral().size() == 1U) {
    auto dots_tokens = ctx->getTokens(kDotsTokenId);
    long long bound = ctx->integerLiteral()[0]->accept(this).as<long long>();
    if (!dots_tokens.size()) {
      // -[*2]-
      return std::pair<long long, long long>(bound, bound);
    }
    if (dots_tokens[0]->getSourceInterval().startsAfter(
            ctx->integerLiteral()[0]->getSourceInterval())) {
      // -[*2..]-
      return std::pair<long long, long long>(bound, LLONG_MAX);
    } else {
      // -[*..2]-
      return std::pair<long long, long long>(1LL, bound);
    }
  } else {
    long long lbound = ctx->integerLiteral()[0]->accept(this).as<long long>();
    long long rbound = ctx->integerLiteral()[1]->accept(this).as<long long>();
    // -[*2..5]-
    return std::pair<long long, long long>(lbound, rbound);
  }
}

antlrcpp::Any
CypherMainVisitor::visitExpression(CypherParser::ExpressionContext *ctx) {
  return visitChildren(ctx);
}

// OR.
antlrcpp::Any
CypherMainVisitor::visitExpression12(CypherParser::Expression12Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression11(), DataStructures::ExpressionOp::LOGICAL_OR);
}

// XOR.
antlrcpp::Any
CypherMainVisitor::visitExpression11(CypherParser::Expression11Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression10(), DataStructures::ExpressionOp::LOGICAL_XOR);
}

// AND.
antlrcpp::Any
CypherMainVisitor::visitExpression10(CypherParser::Expression10Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression9(), DataStructures::ExpressionOp::LOGICAL_AND);
}

// NOT.
antlrcpp::Any
CypherMainVisitor::visitExpression9(CypherParser::Expression9Context *ctx) {
  // TODO: make template similar to LeftAssociativeOperatorExpression for unary
  // expresssions.
  auto operand = ctx->expression8()->accept(this).as<std::pair<Operand, int>>();
  for (int i = 0; i < (int)ctx->NOT().size(); ++i) {
    auto expression =
        ds_.AddExpression(DataStructures::ExpressionOp::LOGICAL_NOT, {operand});
    operand = {Operand::EXPRESSION, expression.first};
  }
  return operand;
}

// Comparisons.
antlrcpp::Any
CypherMainVisitor::visitExpression8(CypherParser::Expression8Context *ctx) {
  if (!ctx->partialComparisonExpression().size()) {
    // There is no comparison operators. We generate expression7.
    return ctx->expression7()->accept(this);
  }

  // There is at least one comparison. We need to generate code for each of
  // them. We don't call visitPartialComparisonExpression but do everything in
  // this function and call expression7-s directly. Since every expression7
  // can be generated twice (because it can appear in two comparisons) code
  // generated by whole subtree of expression7 must not have any sideeffects.
  // We handle chained comparisons as defined by mathematics, neo4j handles
  // them in a very interesting, illogical and incomprehensible way. For
  // example in neo4j:
  //  1 < 2 < 3 -> true,
  //  1 < 2 < 3 < 4 -> false,
  //  5 > 3 < 5 > 3 -> true,
  //  4 <= 5 < 7 > 6 -> false
  //  All of those comparisons evaluate to true in memgraph.
  std::vector<std::pair<Operand, int>> children_ids;
  children_ids.push_back(
      ctx->expression7()->accept(this).as<std::pair<Operand, int>>());
  auto partial_comparison_expressions = ctx->partialComparisonExpression();
  for (auto *child : partial_comparison_expressions) {
    children_ids.push_back(child->accept(this).as<std::pair<Operand, int>>());
  }

  // Make all comparisons.
  auto first_operand = children_ids[0];
  std::vector<std::pair<Operand, int>> comparison_ids;
  for (int i = 0; i < (int)partial_comparison_expressions.size(); ++i) {
    auto *expr = partial_comparison_expressions[i];
    auto op = [](CypherParser::PartialComparisonExpressionContext *expr) {
      if (expr->getToken(kEqTokenId, 0)) {
        return DataStructures::ExpressionOp::EQ;
      } else if (expr->getToken(kNeTokenId1, 0) ||
                 expr->getToken(kNeTokenId2, 0)) {
        return DataStructures::ExpressionOp::NE;
      } else if (expr->getToken(kLtTokenId, 0)) {
        return DataStructures::ExpressionOp::LT;
      } else if (expr->getToken(kGtTokenId, 0)) {
        return DataStructures::ExpressionOp::GT;
      } else if (expr->getToken(kLeTokenId, 0)) {
        return DataStructures::ExpressionOp::LE;
      } else if (expr->getToken(kGeTokenId, 0)) {
        return DataStructures::ExpressionOp::GE;
      }
      assert(false);
      return DataStructures::ExpressionOp::GE;
    }(expr);
    auto expression =
        ds_.AddExpression(op, {first_operand, children_ids[i + 1]});
    first_operand = {Operand::EXPRESSION, expression.first};
    comparison_ids.push_back(first_operand);
  }

  first_operand = comparison_ids[0];
  // Calculate logical and of results of comparisons.
  for (int i = 1; i < (int)comparison_ids.size(); ++i) {
    auto expression =
        ds_.AddExpression(DataStructures::ExpressionOp::LOGICAL_AND,
                          {first_operand, comparison_ids[i]});
    first_operand = {Operand::EXPRESSION, expression.first};
  }
  return first_operand;
}

antlrcpp::Any CypherMainVisitor::visitPartialComparisonExpression(
    CypherParser::PartialComparisonExpressionContext *) {
  debug_assert(false, "Should never be called. See documentation in hpp.");
  return 0;
}

// Addition and subtraction.
antlrcpp::Any
CypherMainVisitor::visitExpression7(CypherParser::Expression7Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression6(),
      MapTokensToOperators(
          ctx, {{kPlusTokenId, DataStructures::ExpressionOp::ADDITION},
                {kMinusTokenId, DataStructures::ExpressionOp::SUBTRACTION}}));
}

// Multiplication, division, modding.
antlrcpp::Any
CypherMainVisitor::visitExpression6(CypherParser::Expression6Context *ctx) {
  return LeftAssociativeOperatorExpression(
      ctx->expression5(),
      MapTokensToOperators(
          ctx, {{kMultTokenId, DataStructures::ExpressionOp::MULTIPLICATION},
                {kDivTokenId, DataStructures::ExpressionOp::DIVISION},
                {kModTokenId, DataStructures::ExpressionOp::MODULO}}));
}

// Power.
antlrcpp::Any
CypherMainVisitor::visitExpression5(CypherParser::Expression5Context *ctx) {
  if (ctx->expression4().size() > 1u) {
    // TODO: implement power operator. In neo4j power is right associative and
    // int^int -> float.
    throw SemanticException();
  }
  return visitChildren(ctx);
}

// Unary minus and plus.
antlrcpp::Any
CypherMainVisitor::visitExpression4(CypherParser::Expression4Context *ctx) {
  auto ops = MapTokensToOperators(
      ctx, {{kUnaryPlusTokenId, DataStructures::ExpressionOp::UNARY_PLUS},
            {kUnaryMinusTokenId, DataStructures::ExpressionOp::UNARY_MINUS}});
  auto operand = ctx->expression3()->accept(this).as<std::pair<Operand, int>>();
  for (int i = 0; i < (int)ops.size(); ++i) {
    auto expression = ds_.AddExpression(ops[i], {operand});
    operand = {Operand::EXPRESSION, expression.first};
  }
  return operand;
}

antlrcpp::Any
CypherMainVisitor::visitExpression3(CypherParser::Expression3Context *ctx) {
  // If there is only one child we don't need to generate any code in this since
  // that child is expression2. Other operations are not implemented at the
  // moment.
  // TODO: implement this.
  if (ctx->children.size() > 1u) {
    throw SemanticException();
  }
  return visitChildren(ctx);
}

antlrcpp::Any
CypherMainVisitor::visitExpression2(CypherParser::Expression2Context *ctx) {
  if (ctx->nodeLabels().size()) {
    // TODO: Implement this. We don't currently support label checking in
    // expresssion.
    throw SemanticException();
  }
  auto operand = ctx->atom()->accept(this).as<std::pair<Operand, int>>();
  for (int i = 0; i < (int)ctx->propertyLookup().size(); ++i) {
    auto property_key = ctx->propertyLookup()[i]->getText();
    auto expression = ds_.AddExpression(
        DataStructures::ExpressionOp::PROPERTY_GETTER,
        {operand, {Operand::PROPERTY, ds_.GetPropertyIndex(property_key)}});
    operand = {Operand::EXPRESSION, expression.first};
  }
  return operand;
}

antlrcpp::Any CypherMainVisitor::visitAtom(CypherParser::AtomContext *ctx) {
  if (ctx->literal()) {
    // THIS COMMENT IS OBSOLETE, NO LITERALS SHOULD APPEAR.
    // This is not very nice since we didn't parse text given in query, but we
    // left that job to the code generator. Correct approach would be to parse
    // it and store it in a structure of appropriate type, int, string... And
    // then code generator would generate its own text based on structure. This
    // is also a security risk if code generator doesn't parse and escape
    // text appropriately. At the moment we don't care much since literal will
    // appear only in tests and real queries will be stripped.
    // TODO: Either parse it correctly or raise exception. If exception is
    // raised it tests should also use params instead of literals.
    throw SemanticException();
  } else if (ctx->parameter()) {
    // This is once again potential security risk. We shouldn't output text
    // given in user query as parameter name directly to the code. Stripper
    // should either replace user's parameter name with generic one or we should
    // allow only parameters with numeric names. At the moment this is not a
    // problem since we don't accept user's parameters but only ours.
    // TODO: revise this.
    auto text = ctx->literal()->getText();
    auto expression =
        ds_.AddExpression(DataStructures::ExpressionOp::PARAMETER,
                          {{Operand::PARAMETER, ds_.GetParamIndex(text)}});
    return expression.first;
  } else if (ctx->parenthesizedExpression()) {
    return ctx->parenthesizedExpression()->accept(this);
  } else if (ctx->variable()) {
    auto variable = ctx->variable()->accept(this).as<std::pair<Operand, int>>();
    auto expression =
        ds_.AddExpression(DataStructures::ExpressionOp::VARIABLE, {variable});
    return std::pair<Operand, int>(Operand::EXPRESSION, expression.first);
  }
  // TODO: Implement this. We don't support comprehensions, functions,
  // filtering... at the moment.
  throw SemanticException();
}

antlrcpp::Any CypherMainVisitor::visitIntegerLiteral(
    CypherParser::IntegerLiteralContext *ctx) {
  long long t = 0LL;
  try {
    t = std::stoll(ctx->getText(), 0, 0);
  } catch (std::out_of_range) {
    throw SemanticException();
  }
  return t;
}
}
}
