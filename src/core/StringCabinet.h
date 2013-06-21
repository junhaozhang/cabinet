/**
 * Copyright 2013 i-MD.com. All Rights Reserved.
 *
 * String Cabinet Header
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#ifndef CABINET_CORE_STRING_CABINET_H_
#define CABINET_CORE_STRING_CABINET_H_

#include <functional>

namespace cabinet {
  struct StringKeyHeaderExtractor : public std::unary_functional<uint32_t, std::string> {
    uint32_t operator()(std::string& str) const {
      return htole32((uint32_t)str.size());
    } 
  };

  struct StringKeyDataExtractor : public std::unary_functional<KeyData, std::string> {
    KeyData operator()(std::string& str) const {
      KeyData ret = { str.c_str(), (uint32_t)str.size() };
      return ret;
    }
  };

  struct StringKeyReader : public std::binary_functional<std::string, FILE*, uint32_t> {
    std::string operator()(FILE* file, uint32_t keyheader) const {
      uint32_t size = letoh32(keyheader);
      std::string str(size, '\0');
      if (fread(str.c_str(), size + 1, 1, file) != 1) {
        throw std::io_exception();
        return "";
      }
      if (str.c_str[size] != '\0') {
        throw FileCorruptException();
        return "";
      }
      return str;
    }
  };

  typedef Cabinet<String, uint32_t, StringKeyHeaderExtractor, StringKeyDataExtractor, StringKeyReader> StringCabinet;
}  // namespace cabinet

#endif  // CABINET_CORE_STRING_CABINET_H_
