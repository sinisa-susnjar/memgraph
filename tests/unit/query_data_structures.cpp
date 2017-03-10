//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#include <memory>

#include "gtest/gtest.h"

#include "query/backend/data_structures.hpp"


using namespace query;


TEST(QueryDataStructures, Variables) {
  DataStructures ds;

  auto var1 = ds.GetVariableIndex("var1");
  auto var2 = ds.GetVariableIndex("var2");

  EXPECT_NE(var1, var2);
  EXPECT_EQ(var1, ds.GetVariableIndex("var1"));
  EXPECT_EQ(ds.variables()[var1], "var1");
}

TEST(QueryDataStructures, Properties) {
  DataStructures ds;

  auto prop1 = ds.GetPropertyIndex("prop1");
  auto prop2 = ds.GetPropertyIndex("prop2");

  EXPECT_NE(prop1, prop2);
  EXPECT_EQ(prop1, ds.GetPropertyIndex("prop1"));
  EXPECT_EQ(ds.properties()[prop1], "prop1");
}

TEST(QueryDataStructures, Labels) {
  DataStructures ds;

  auto label1 = ds.GetLabelIndex("label1");
  auto label2 = ds.GetLabelIndex("label2");

  EXPECT_NE(label1, label2);
  EXPECT_EQ(label1, ds.GetLabelIndex("label1"));
  EXPECT_EQ(ds.labels()[label1], "label1");
}

TEST(QueryDataStructures, EdgeTypes) {
  DataStructures ds;

  auto edge_type1 = ds.GetEdgeTypeIndex("edge_type1");
  auto edge_type2 = ds.GetEdgeTypeIndex("edge_type2");

  EXPECT_NE(edge_type1, edge_type2);
  EXPECT_EQ(edge_type1, ds.GetEdgeTypeIndex("edge_type1"));
  EXPECT_EQ(ds.edge_types()[edge_type1], "edge_type1");
}

TEST(QueryDataStructures, Expression) {
  DataStructures ds;

  auto expr1 = ds.AddExpression(DataStructures::ExpressionOp::ADDITION);
  EXPECT_EQ(expr1.second.op_, DataStructures::ExpressionOp::ADDITION);
  EXPECT_EQ(ds.expressions().size(), 1);

  auto expr2 = ds.AddExpression(DataStructures::ExpressionOp::SUBTRACTION);
  expr2.second.operands_.emplace_back(DataStructures::ExpressionOperand ::EXPRESSION, 42);
  EXPECT_EQ(expr2.second.operands_[0].first, DataStructures::ExpressionOperand::EXPRESSION);
  EXPECT_EQ(expr2.second.operands_[0].second, 42);
}

TEST(QueryDataStructures, Pattern) {
  DataStructures ds;

  auto start_node = DataStructures::Node();
  EXPECT_EQ(ds.patterns().size(), 0);
  ds.AddPattern(start_node);
  EXPECT_EQ(ds.patterns().size(), 1);
}

TEST(QueryDataStructures, MatchClause) {
  DataStructures ds;

  EXPECT_EQ(ds.clauses().size(), 0);
  auto match = ds.AddMatch();
  EXPECT_EQ(ds.clauses().size(), 1);
  EXPECT_EQ(-1, match.second.where_expression_);
}
