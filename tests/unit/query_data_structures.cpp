//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//
#include "gtest/gtest.h"

#include "query/data_structures.hpp"


TEST(QueryDataStructures, Variables) {
  DataStructures ds;

  auto var1 = ds.GetVariableIndex("var1");
  auto var2 = ds.GetVariableIndex("var2");

  EXPECT_NE(var1, var2);
  EXPECT_EQ(var1, ds.GetVariableIndex("var1"));
  EXPECT_EQ(ds.Variables()[var1].name_, "var1");
}

TEST(QueryDataStructures, Properties) {
  DataStructures ds;

  auto prop1 = ds.GetPropertyIndex("prop1");
  auto prop2 = ds.GetPropertyIndex("prop2");

  EXPECT_NE(prop1, prop2);
  EXPECT_EQ(prop1, ds.GetPropertyIndex("prop1"));
  EXPECT_EQ(ds.Properties()[prop1].name_, "prop1");
}

TEST(QueryDataStructures, Labels) {
  DataStructures ds;

  auto label1 = ds.GetLabelIndex("label1");
  auto label2 = ds.GetLabelIndex("label2");

  EXPECT_NE(label1, label2);
  EXPECT_EQ(label1, ds.GetLabelIndex("label1"));
  EXPECT_EQ(ds.Labels()[label1].name_, "label1");
}

TEST(QueryDataStructures, EdgeTypes) {
  DataStructures ds;

  auto edge_type1 = ds.GetEdgeTypeIndex("edge_type1");
  auto edge_type2 = ds.GetEdgeTypeIndex("edge_type2");

  EXPECT_NE(edge_type1, edge_type2);
  EXPECT_EQ(edge_type1, ds.GetEdgeTypeIndex("edge_type1"));
  EXPECT_EQ(ds.EdgeTypes()[edge_type1].name_, "edge_type1");
}

TEST(QueryDataStructures, Expression) {
  DataStructures ds;

  auto expr1 = ds.AddExpression(DataStructures::ExpressionOp::Addition);
  auto expr2 = ds.AddExpression(DataStructures::ExpressionOp::Subtraction);

  EXPECT_NE(expr1, expr2);
  EXPECT_EQ(ds.Expressions()[expr1].op_, DataStructures::ExpressionOp::Addition);

  auto expr3 = ds.AddExpression(
      DataStructures::ExpressionOp::Addition,
      DataStructures::ExpressionOperand::Expression, 42);
  EXPECT_EQ(ds.Expressions()[expr3].operands_[0].first, DataStructures::ExpressionOperand::Expression);
  EXPECT_EQ(ds.Expressions()[expr3].operands_[0].second, 42);

  auto expr4 = ds.AddExpression(
      DataStructures::ExpressionOp::Addition,
      DataStructures::ExpressionOperand::Expression, 4,
      DataStructures::ExpressionOperand::Expression, 2);
  EXPECT_NE(expr3, expr4);
  EXPECT_EQ(ds.Expressions()[expr4].operands_[0].second, 4);
  EXPECT_EQ(ds.Expressions()[expr4].operands_[1].second, 2);
}

TEST(QueryDataStructures, Pattern) {
  DataStructures ds;

  auto p1 = ds.AddPattern(DataStructures::Pattern());
  auto p2 = ds.AddPattern(DataStructures::Pattern());
  EXPECT_NE(p1, p2);
  EXPECT_EQ(ds.Patterns().size(), 2);
}

TEST(QueryDataStructures, Clause) {
  DataStructures ds;

  EXPECT_EQ(ds.Clauses().size(), 0);
  auto &clause1 = ds.AddClause(DataStructures::ClauseType::Merge);
  clause1.elements_.push_back(42);
  clause1.elements_.push_back(17);
  EXPECT_EQ(clause1.type_, DataStructures::ClauseType::Merge);
  EXPECT_EQ(clause1.elements_.size(), 2);
  EXPECT_EQ(ds.Clauses().size(), 1);
}
