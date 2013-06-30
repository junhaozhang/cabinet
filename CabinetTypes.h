/**
 * Copyright 2013 i-MD.com. All Rights Reserved.
 *
 * Various Cabinet Types.
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#ifndef CABINET_TYPES_H_
#define CABINET_TYPES_H_

#include <functional>
#include <exception>
#include <stdexcept>
#include "TCabinet.tcc"

// Currently there are three types of cabinet instance:
// U32Cabinet: store with uint32_t keys.
// U64Cabinet: store with uint64_t keys.
// StringCabinet: store with string keys.
namespace cabinet {
  struct U32KeyReader : public std::binary_function<bool, FILE*, uint32_t&> {
    uint32_t operator()(FILE* file, uint32_t& ret) const {
      if (fread(&ret, sizeof(ret), 1, file) != 1) {
        return false;
      }
      ret = le32toh(ret);
      return true;
    }
  };

  struct U32KeyWriter : public std::binary_function<void, FILE*, const uint32_t&> {
    void operator()(FILE* file, uint32_t key) const {
      key = htole32(key);
      if (fwrite(&key, sizeof(key), 1, file) != 1) {
        int err = errno;
        fclose(file);
        throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
      }
    }
  };
 
  typedef TCabinet<uint32_t, U32KeyReader, U32KeyWriter> U32Cabinet;

  struct U64KeyReader : public std::binary_function<bool, FILE*, uint64_t&> {
    bool operator()(FILE* file, uint64_t& ret) const {
      if (fread(&ret, sizeof(ret), 1, file) != 1) {
        return false;
      }
      ret = le64toh(ret);
      return true;
    }
  };

  struct U64KeyWriter : public std::binary_function<void, FILE*, uint64_t> {
    void operator()(FILE* file, uint64_t key) const {
      key = htole64(key);
      if (fwrite(&key, sizeof(key), 1, file) != 1) {
        int err = errno;
        fclose(file);
        throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
      }
    }
  };
 
  typedef TCabinet<uint64_t, U64KeyReader, U64KeyWriter> U64Cabinet;

  struct StringKeyReader : public std::binary_function<bool, FILE*, std::string&> {
    bool operator()(FILE* file, std::string& ret) const {
      uint32_t size;
      if (fread(&size, sizeof(size), 1, file) != 1) {
        return false;
      }
      size = le32toh(size);
      ret.resize(size);
      if (size > 0 && fread((void*)ret.c_str(), size, 1, file) != 1) {
        int err = errno;
        fclose(file);
        throw ReadFileException(__FILE__, __LINE__, err, strerror(err));
      }
      return true;
    }
  };

  struct StringKeyWriter : public std::binary_function<void, FILE*,const std::string&> {
    void operator()(FILE* file, const std::string& ret) const {
      if (ret.size() > (uint64_t)0xffffffff) {
        throw std::runtime_error("String too large!");
      }
      uint32_t size = htole32((uint32_t)ret.size());
      if (fwrite(&size, sizeof(size), 1, file) != 1 || fwrite(ret.c_str(), size, 1, file) != 1) {
        int err = errno;
        fclose(file);
        throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
      }
    }
  };

  struct StringHashFunc : public std::unary_function<size_t, const std::string &> {
    size_t operator()(const std::string& str) const {
      return __gnu_cxx::hash<const char*>()(str.c_str());
    }
  };

  typedef TCabinet<std::string, StringKeyReader, StringKeyWriter, StringHashFunc> StringCabinet;
}  // namespace cabinet

#endif  // CABINET_TYPES_H_
