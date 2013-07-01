/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * U32 Cabinet Unit Test
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#define BOOST_TEST_MODULE u32cabinet_test

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <boost/test/included/unit_test.hpp>

#include "CabinetTypes.h"

using cabinet::StringCabinet;

static const char* cab_path = "stringcab";
static uint32_t times = 10000;

struct TestFixture {
  TestFixture() {
  }
  ~TestFixture() {
    std::string cmdline = "rm -rf ";
    cmdline += cab_path;
    system(cmdline.c_str());
  }
};

std::string u32tostr(uint32_t u) {
  std::stringstream oss;
  oss << u;
  return oss.str();
}

BOOST_AUTO_TEST_SUITE(test)

// test case 1
// writing several records, check reading results.
BOOST_FIXTURE_TEST_CASE(test_case_1, TestFixture) {
  time_t old_time = time(NULL);
  StringCabinet cab(cab_path);
  fprintf(stderr, "Startup using %lld second(s).\n", time(NULL) - old_time);

  old_time = time(NULL);
  uint8_t buffer[20 * 1024];
  for (uint32_t i = 0; i < times; ++i) {
    uint32_t size = (~i) % sizeof(buffer);
    memset(buffer, (uint8_t)((~i) % 37), size);
    cab.Set(u32tostr(i), buffer, size);
  }
  fprintf(stderr, "Putting %d items, using %lld second(s).\n", times, time(NULL) - old_time);

  // sequential reading performance.
  old_time = time(NULL);
  std::string value;
  for (uint32_t i = 0; i < times; ++i) {
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == ( (~i) % sizeof(buffer)) );
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 37));
    }
  }
  fprintf(stderr, "Sequential Getting %d items, using %lld second(s).\n", times, time(NULL) - old_time);

  // random reads.
  std::vector<uint32_t> vec;
  for (uint32_t i = 0; i < times; ++i) {
    vec.push_back(i);
  }
  srand(0);
  std::random_shuffle(vec.begin(), vec.end());

  old_time = time(NULL);
  uint32_t j;
  for (uint32_t i = 0; i < times / 10; ++i) {
    if ((i % 1000) == 0) {
      fprintf(stderr, "LOOP %d:\n", i);
    }
    j = vec[i];
    BOOST_REQUIRE(cab.Get(u32tostr(j), &value));
    BOOST_REQUIRE(value.size() == ( (~j) % sizeof(buffer)) );
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~j) % 37));
    }
  }
  fprintf(stderr, "Random Getting %d items, using %lld second(s).\n",
    times / 10, time(NULL) - old_time);

  old_time = time(NULL);
  cab.Close();
  fprintf(stderr, "Close using %lld second(s).\n", time(NULL) - old_time);
}

// test case 2
// test load data.
BOOST_FIXTURE_TEST_CASE(test_case_2, TestFixture) {
  StringCabinet cab(cab_path);

  uint8_t buffer[20 * 1024];
  for (uint32_t i = 0; i < times; ++i) {
    uint32_t size = (~i) % sizeof(buffer);
    memset(buffer, (uint8_t)((~i) % 37), size);
    cab.Set(u32tostr(i), buffer, size);
  }

  cab.Close();
  cab.Open(cab_path);

  BOOST_REQUIRE(cab.GetEntryCount() == times);
  std::string value;
  for (uint32_t i = 0; i < times; ++i) {
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == (~i) % sizeof(buffer));
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 37));
    }
  }
  cab.Close();
}

// test case 3
// test Set => Delete => Replace flow.
BOOST_FIXTURE_TEST_CASE(test_case_3, TestFixture) {
  StringCabinet cab;
  cab.Open(cab_path);

  uint8_t buffer[27367];
  for (uint32_t i = 0; i < times; ++i) {
    uint32_t size = (~i) % (27367);
    // printf("INSERT #%d, size=%d, byte=%d\n",
    //  i, size, (uint32_t)(uint8_t)((~i) % 37));
    memset(buffer, (uint8_t)((~i) % 37), size);
    cab.Set(u32tostr(i), buffer, size);
  }

  for (uint32_t i = (uint32_t)(times * 0.6 + 0.5);
    i < (uint32_t)(times * 0.8 + 0.5); ++i) {
    // printf("DELETE #%d\n", i);
    cab.Delete(u32tostr(i));
  }

  for (uint32_t i = (uint32_t)(times * 0.8 + 0.5); i < times; ++i) {
    uint32_t size = (~i) % (20 * 1024);
    // printf("REPLACE #%d, size=%d, byte=%d\n",
    //  i, size, (uint32_t)(uint8_t)((~i) % 131));
    memset(buffer, (uint8_t)((~i) % 131), size);
    cab.Set(u32tostr(i), buffer, size);
  }

  std::string value;
  for (uint32_t i = 0; i < (uint32_t)(times * 0.6 + 0.5); ++i) {
    // fprintf(stderr, "#Get %d\n", i);
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == (~i) % 27367);
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 37));
      BOOST_REQUIRE((uint8_t)value[0] == (uint8_t)((~i) % 37));
      BOOST_REQUIRE((uint8_t)value[value.size() - 1] == (uint8_t)((~i) % 37));
    }
  }

  for (uint32_t i = (uint32_t)(times * 0.6 + 0.5);
    i < (uint32_t)(times * 0.8 + 0.5); ++i) {
    BOOST_REQUIRE(!cab.Get(u32tostr(i), &value));
  }

  for (uint32_t i = (uint32_t)(times * 0.8 + 0.5); i < times; ++i) {
    // fprintf(stderr, "#Get %d\n", i);
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == (~i) % (20 * 1024));
    if (!value.empty()) {
      // fprintf(stderr, "#%d, actual=%d, expected=%d\n",
      //  i, (uint32_t)(uint8_t)value[value.size() / 2],
      //  (uint32_t)(uint8_t)((~i) % 131));
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 131));
      BOOST_REQUIRE((uint8_t)value[0] == (uint8_t)((~i) % 131));
      BOOST_REQUIRE((uint8_t)value[value.size() - 1] == (uint8_t)((~i) % 131));
    }
  }

  cab.Close();
}

// test case 4
// based on test case 3，test compact operation.
BOOST_FIXTURE_TEST_CASE(test_case_4, TestFixture) {
  StringCabinet cab(cab_path);

  uint8_t buffer[27367];
  for (uint32_t i = 0; i < times; ++i) {
    uint32_t size = (~i) % (20 * 1024);
    memset(buffer, (uint8_t)((~i) % 37), size);
    cab.Set(u32tostr(i), buffer, size);
  }

  for (uint32_t i = (uint32_t)(times * 0.6 + 0.5);
    i < (uint32_t)(times * 0.8 + 0.5); ++i) {
    cab.Delete(u32tostr(i));
  }

  for (uint32_t i = (uint32_t)(times * 0.8 + 0.5); i < times; ++i) {
    uint32_t size = (~i) % 27367;
    memset(buffer, (uint8_t)((~i) % 131), size);
    cab.Set(u32tostr(i), buffer, size);
  }

  time_t old_time = time(NULL);
  cab.Compact();
  fprintf(stderr,
    "Compact %d recs used %d secs.\n",
    (uint32_t)(0.8 * times),
    time(NULL) - old_time);
  BOOST_REQUIRE(cab.GetDataBytes() == cab.GetDataFileSize());
  std::string value;
  for (uint32_t i = 0; i < (uint32_t)(times * 0.6 + 0.5); ++i) {
    // fprintf(stderr, "%d\n", i);
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == (~i) % (20 * 1024));
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[0] == (uint8_t)((~i) % 37));
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 37));
    }
  }

  for (uint32_t i = (uint32_t)(times * 0.6 + 0.5);
    i < (uint32_t)(times * 0.8 + 0.5); ++i) {
    BOOST_REQUIRE(!cab.Get(u32tostr(i), &value));
  }

  for (uint32_t i = (uint32_t)(times * 0.8 + 0.5); i < times; ++i) {
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == (~i) % 27367);
    if (value.size() > 0) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 131));
    }
  }
  cab.Close();
}

// test memory get
BOOST_FIXTURE_TEST_CASE(test_case_5, TestFixture) {
  time_t old_time = time(NULL);
  StringCabinet cab(cab_path);
  fprintf(stderr, "Startup using %lld second(s).\n", time(NULL) - old_time);

  old_time = time(NULL);
  uint8_t buffer[20 * 1024];
  std::string value;
  for (uint32_t i = 0; i < times; ++i) {
    uint32_t size = (~i) % sizeof(buffer);
    memset(buffer, (uint8_t)((~i) % 37), size);
    cab.Set(u32tostr(i), buffer, size);
    BOOST_REQUIRE(cab.Get(u32tostr(i), &value));
    BOOST_REQUIRE(value.size() == ( (~i) % sizeof(buffer)) );
    if (!value.empty()) {
      BOOST_REQUIRE((uint8_t)value[value.size() / 2] == (uint8_t)((~i) % 37));
    }
  }
  printf("Put&Set %d items, using %lld second(s).\n",
    times, time(NULL) - old_time);
}

BOOST_AUTO_TEST_SUITE_END()
