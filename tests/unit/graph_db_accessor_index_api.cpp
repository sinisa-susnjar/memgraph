#include <experimental/optional>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "database/graph_db_accessor.hpp"
#include "database/dbms.hpp"
#include "utils/bound.hpp"

using testing::UnorderedElementsAreArray;

template <typename TIterable>
auto Count(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}

TEST(GraphDbAccessor, VertexByLabelCount) {
  Dbms dbms;
  auto dba = dbms.active();
  auto lab1 = dba->label("lab1");
  auto lab2 = dba->label("lab2");

  EXPECT_EQ(dba->vertices_count(lab1), 0);
  EXPECT_EQ(dba->vertices_count(lab2), 0);
  EXPECT_EQ(dba->vertices_count(), 0);
  for (int i = 0; i < 11; ++i) dba->insert_vertex().add_label(lab1);
  for (int i = 0; i < 17; ++i) dba->insert_vertex().add_label(lab2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->vertices_count(lab1), 11);
  EXPECT_EQ(dba->vertices_count(lab2), 17);
  EXPECT_EQ(dba->vertices_count(), 28);
}

TEST(GraphDbAccessor, VertexByLabelPropertyCount) {
  Dbms dbms;
  auto dba = dbms.active();

  auto lab1 = dba->label("lab1");
  auto lab2 = dba->label("lab2");

  auto prop1 = dba->property("prop1");
  auto prop2 = dba->property("prop2");

  dba->BuildIndex(lab1, prop1);
  dba->BuildIndex(lab1, prop2);
  dba->BuildIndex(lab2, prop1);
  dba->BuildIndex(lab2, prop2);

  EXPECT_EQ(dba->vertices_count(lab1, prop1), 0);
  EXPECT_EQ(dba->vertices_count(lab1, prop2), 0);
  EXPECT_EQ(dba->vertices_count(lab2, prop1), 0);
  EXPECT_EQ(dba->vertices_count(lab2, prop2), 0);
  EXPECT_EQ(dba->vertices_count(), 0);

  for (int i = 0; i < 14; ++i) {
    VertexAccessor vertex = dba->insert_vertex();
    vertex.add_label(lab1);
    vertex.PropsSet(prop1, 1);
  }
  for (int i = 0; i < 15; ++i) {
    auto vertex = dba->insert_vertex();
    vertex.add_label(lab1);
    vertex.PropsSet(prop2, 2);
  }
  for (int i = 0; i < 16; ++i) {
    auto vertex = dba->insert_vertex();
    vertex.add_label(lab2);
    vertex.PropsSet(prop1, 3);
  }
  for (int i = 0; i < 17; ++i) {
    auto vertex = dba->insert_vertex();
    vertex.add_label(lab2);
    vertex.PropsSet(prop2, 4);
  }
  // even though xxx_count functions in GraphDbAccessor can over-estimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->vertices_count(lab1, prop1), 14);
  EXPECT_EQ(dba->vertices_count(lab1, prop2), 15);
  EXPECT_EQ(dba->vertices_count(lab2, prop1), 16);
  EXPECT_EQ(dba->vertices_count(lab2, prop2), 17);
  EXPECT_EQ(dba->vertices_count(), 14 + 15 + 16 + 17);
}

#define EXPECT_WITH_MARGIN(x, center) \
  EXPECT_THAT(                        \
      x, testing::AllOf(testing::Ge(center - 2), testing::Le(center + 2)));

TEST(GraphDbAccessor, VertexByLabelPropertyValueCount) {
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("label");
  auto property = dba->property("property");
  dba->BuildIndex(label, property);

  // add some vertices without the property
  for (int i = 0; i < 20; i++) dba->insert_vertex();

  // add vertices with prop values [0, 29), ten vertices for each value
  for (int i = 0; i < 300; i++) {
    auto vertex = dba->insert_vertex();
    vertex.add_label(label);
    vertex.PropsSet(property, i / 10);
  }
  // add verties in t he [30, 40) range, 100 vertices for each value
  for (int i = 0; i < 1000; i++) {
    auto vertex = dba->insert_vertex();
    vertex.add_label(label);
    vertex.PropsSet(property, 30 + i / 100);
  }

  // test estimates for exact value count
  EXPECT_WITH_MARGIN(dba->vertices_count(label, property, 10), 10);
  EXPECT_WITH_MARGIN(dba->vertices_count(label, property, 14), 10);
  EXPECT_WITH_MARGIN(dba->vertices_count(label, property, 30), 100);
  EXPECT_WITH_MARGIN(dba->vertices_count(label, property, 39), 100);
  EXPECT_EQ(dba->vertices_count(label, property, 40), 0);

  // helper functions
  auto Inclusive = [](int64_t value) {
    return std::experimental::make_optional(
        utils::MakeBoundInclusive(PropertyValue(value)));
  };
  auto Exclusive = [](int64_t value) {
    return std::experimental::make_optional(
        utils::MakeBoundExclusive(PropertyValue(value)));
  };
  auto Count = [&dba, label, property](auto lower, auto upper) {
    return dba->vertices_count(label, property, lower, upper);
  };

  using std::experimental::nullopt;
  EXPECT_DEATH(Count(nullopt, nullopt), "bound must be provided");
  EXPECT_WITH_MARGIN(Count(nullopt, Exclusive(4)), 40);
  EXPECT_WITH_MARGIN(Count(nullopt, Inclusive(4)), 50);
  EXPECT_WITH_MARGIN(Count(Exclusive(13), nullopt), 160 + 1000);
  EXPECT_WITH_MARGIN(Count(Inclusive(13), nullopt), 170 + 1000);
  EXPECT_WITH_MARGIN(Count(Inclusive(13), Exclusive(14)), 10);
  EXPECT_WITH_MARGIN(Count(Exclusive(13), Inclusive(14)), 10);
  EXPECT_WITH_MARGIN(Count(Exclusive(13), Exclusive(13)), 0);
  EXPECT_WITH_MARGIN(Count(Inclusive(20), Exclusive(13)), 0);
}

#undef EXPECT_WITH_MARGIN

TEST(GraphDbAccessor, EdgeByEdgeTypeCount) {
  Dbms dbms;
  auto dba = dbms.active();
  auto t1 = dba->edge_type("t1");
  auto t2 = dba->edge_type("t2");

  EXPECT_EQ(dba->edges_count(t1), 0);
  EXPECT_EQ(dba->edges_count(t2), 0);
  EXPECT_EQ(dba->edges_count(), 0);
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  for (int i = 0; i < 11; ++i) dba->insert_edge(v1, v2, t1);
  for (int i = 0; i < 17; ++i) dba->insert_edge(v1, v2, t2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->edges_count(t1), 11);
  EXPECT_EQ(dba->edges_count(t2), 17);
  EXPECT_EQ(dba->edges_count(), 28);
}

// Check if build index adds old vertex entries (ones before the index was
// created)
TEST(GraphDbAccessor, BuildIndexOnOld) {
  Dbms dbms;
  auto dba = dbms.active();

  auto label = dba->label("lab1");
  auto property = dba->property("prop1");

  auto vertex_accessor = dba->insert_vertex();
  vertex_accessor.add_label(label);
  vertex_accessor.PropsSet(property, 0);

  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(dba->vertices_count(label, property), "Index doesn't exist.");
  dba->commit();

  auto dba2 = dbms.active();
  dba2->BuildIndex(label, property);
  dba2->commit();

  auto dba3 = dbms.active();
  // Index is built and vertex is automatically added inside
  EXPECT_EQ(dba3->vertices_count(label, property), 1);
  EXPECT_EQ(Count(dba3->vertices(label, property)), 1);
  dba3->commit();
}

// Try to build index two times
TEST(GraphDbAccessor, BuildIndexDouble) {
  Dbms dbms;
  auto dba = dbms.active();

  auto label = dba->label("lab1");
  auto property = dba->property("prop1");
  dba->BuildIndex(label, property);
  EXPECT_THROW(dba->BuildIndex(label, property), utils::BasicException);
}

// Inserts vertices with properties with integers and filters to get exact
// vertices with an exact integer.
TEST(GraphDbAccessor, FilterLabelPropertySpecificValue) {
  Dbms dbms;
  auto dba = dbms.active();
  auto label = dba->label("lab1");
  auto property = dba->property("prop1");
  dba->BuildIndex(label, property);
  dba->commit();

  auto dba2 = dbms.active();
  for (int i = 1; i <= 5; ++i) {
    for (int j = 1; j <= i; ++j) {
      auto vertex = dba2->insert_vertex();
      vertex.add_label(label);
      vertex.PropsSet(property, i);
    }
  }
  dba2->commit();
  auto dba3 = dbms.active();
  for (int i = 1; i <= 5; ++i)
    EXPECT_EQ(Count(dba3->vertices(label, property, PropertyValue(i), false)),
              i);
}

// Inserts integers, double, lists, booleans into index and check if they
// are
// sorted as they should be sorted.
TEST(GraphDbAccessor, SortedLabelPropertyEntries) {
  Dbms dbms;
  auto dba = dbms.active();

  auto label = dba->label("lab1");
  auto property = dba->property("prop1");

  dba->BuildIndex(label, property);
  dba->commit();

  auto dba2 = dbms.active();
  std::vector<PropertyValue> expected_property_value(50, 0);

  // strings
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba2->insert_vertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property,
                             static_cast<std::string>(std::to_string(i)));
    expected_property_value[i] = vertex_accessor.PropsAt(property);
  }
  // bools - insert in reverse to check for comparison between values.
  for (int i = 9; i >= 0; --i) {
    auto vertex_accessor = dba2->insert_vertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, static_cast<bool>(i / 5));
    expected_property_value[10 + i] = vertex_accessor.PropsAt(property);
  }

  // integers
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba2->insert_vertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, i);
    expected_property_value[20 + 2 * i] = vertex_accessor.PropsAt(property);
  }
  // doubles
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba2->insert_vertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, static_cast<double>(i + 0.5));
    expected_property_value[20 + 2 * i + 1] = vertex_accessor.PropsAt(property);
  }

  // lists of ints - insert in reverse to check for comparision between
  // lists.
  for (int i = 9; i >= 0; --i) {
    auto vertex_accessor = dba2->insert_vertex();
    vertex_accessor.add_label(label);
    std::vector<PropertyValue> value;
    value.push_back(PropertyValue(i));
    vertex_accessor.PropsSet(property, value);
    expected_property_value[40 + i] = vertex_accessor.PropsAt(property);
  }

  EXPECT_EQ(Count(dba2->vertices(label, property, false)), 0);
  EXPECT_EQ(Count(dba2->vertices(label, property, true)), 50);

  int cnt = 0;
  for (auto vertex : dba2->vertices(label, property, true)) {
    const PropertyValue &property_value = vertex.PropsAt(property);
    EXPECT_EQ(property_value.type(), expected_property_value[cnt].type());
    switch (property_value.type()) {
      case PropertyValue::Type::Bool:
        EXPECT_EQ(property_value.Value<bool>(),
                  expected_property_value[cnt].Value<bool>());
        break;
      case PropertyValue::Type::Double:
        EXPECT_EQ(property_value.Value<double>(),
                  expected_property_value[cnt].Value<double>());
        break;
      case PropertyValue::Type::Int:
        EXPECT_EQ(property_value.Value<int64_t>(),
                  expected_property_value[cnt].Value<int64_t>());
        break;
      case PropertyValue::Type::String:
        EXPECT_EQ(property_value.Value<std::string>(),
                  expected_property_value[cnt].Value<std::string>());
        break;
      case PropertyValue::Type::List: {
        auto received_value =
            property_value.Value<std::vector<PropertyValue>>();
        auto expected_value =
            expected_property_value[cnt].Value<std::vector<PropertyValue>>();
        EXPECT_EQ(received_value.size(), expected_value.size());
        EXPECT_EQ(received_value.size(), 1);
        EXPECT_EQ(received_value[0].Value<int64_t>(),
                  expected_value[0].Value<int64_t>());
        break;
      }
      case PropertyValue::Type::Null:
        ASSERT_FALSE("Invalid value type.");
    }
    ++cnt;
  }
}

TEST(GraphDbAccessor, VisibilityAfterInsertion) {
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto lab1 = dba->label("lab1");
  auto lab2 = dba->label("lab2");
  v1.add_label(lab1);
  auto type1 = dba->edge_type("type1");
  auto type2 = dba->edge_type("type2");
  dba->insert_edge(v1, v2, type1);

  EXPECT_EQ(Count(dba->vertices(lab1, false)), 0);
  EXPECT_EQ(Count(dba->vertices(lab1, true)), 1);
  EXPECT_EQ(Count(dba->vertices(lab2, false)), 0);
  EXPECT_EQ(Count(dba->vertices(lab2, true)), 0);
  EXPECT_EQ(Count(dba->edges(type1, false)), 0);
  EXPECT_EQ(Count(dba->edges(type1, true)), 1);
  EXPECT_EQ(Count(dba->edges(type2, false)), 0);
  EXPECT_EQ(Count(dba->edges(type2, true)), 0);

  dba->advance_command();

  EXPECT_EQ(Count(dba->vertices(lab1, false)), 1);
  EXPECT_EQ(Count(dba->vertices(lab1, true)), 1);
  EXPECT_EQ(Count(dba->vertices(lab2, false)), 0);
  EXPECT_EQ(Count(dba->vertices(lab2, true)), 0);
  EXPECT_EQ(Count(dba->edges(type1, false)), 1);
  EXPECT_EQ(Count(dba->edges(type1, true)), 1);
  EXPECT_EQ(Count(dba->edges(type2, false)), 0);
  EXPECT_EQ(Count(dba->edges(type2, true)), 0);
}

TEST(GraphDbAccessor, VisibilityAfterDeletion) {
  Dbms dbms;
  auto dba = dbms.active();
  auto lab = dba->label("lab");
  for (int i = 0; i < 5; ++i) dba->insert_vertex().add_label(lab);
  dba->advance_command();
  auto type = dba->edge_type("type");
  for (int j = 0; j < 3; ++j) {
    auto vertices_it = dba->vertices(false).begin();
    dba->insert_edge(*vertices_it++, *vertices_it, type);
  }
  dba->advance_command();

  EXPECT_EQ(Count(dba->vertices(lab, false)), 5);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 5);
  EXPECT_EQ(Count(dba->edges(type, false)), 3);
  EXPECT_EQ(Count(dba->edges(type, true)), 3);

  // delete two edges
  auto edges_it = dba->edges(false).begin();
  for (int k = 0; k < 2; ++k) dba->remove_edge(*edges_it++);
  EXPECT_EQ(Count(dba->edges(type, false)), 3);
  EXPECT_EQ(Count(dba->edges(type, true)), 1);
  dba->advance_command();
  EXPECT_EQ(Count(dba->edges(type, false)), 1);
  EXPECT_EQ(Count(dba->edges(type, true)), 1);

  // detach-delete 2 vertices
  auto vertices_it = dba->vertices(false).begin();
  for (int k = 0; k < 2; ++k) dba->detach_remove_vertex(*vertices_it++);
  EXPECT_EQ(Count(dba->vertices(lab, false)), 5);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 3);
  dba->advance_command();
  EXPECT_EQ(Count(dba->vertices(lab, false)), 3);
  EXPECT_EQ(Count(dba->vertices(lab, true)), 3);
}
