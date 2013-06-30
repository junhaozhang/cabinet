/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Storage Header
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#ifndef CABINET_CABINET_H_
#define CABINET_CABINET_H_

#include <ext/pool_allocator.h>
#include <stdint.h>
#include <hash_map>
#include <hash_set>
#include <functional>
#include <string>
#include <vector>
#include <exception>
#include <sstream>

namespace cabinet {

// we use this superclass for convenience.
// we assume that these interfaces are not called frequently.
class CabinetBase {
 public:
  virtual ~CabinetBase() {};

  virtual void Open(const char* location) = 0;
  virtual void Close() = 0;
  virtual void Drop() = 0;
  virtual void Flush() = 0;
  virtual void Compact() = 0;

  virtual uint64_t GetEntryCount() const = 0;
  virtual uint64_t GetChangedCount() const = 0;
  virtual uint64_t GetDataFileSize() const = 0;
  virtual uint64_t GetDataBytes() const = 0;

  virtual std::string GetPath() const = 0;
};

// class Cabinet
// template params:
// * KeyType
// * KeyReader
// * KeyWriter
// * KeyHashFunc
template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc = __gnu_cxx::hash<KeyType> >
class TCabinet : public CabinetBase {
 public:
  TCabinet();
  explicit TCabinet(const char* location);
  virtual ~TCabinet();

  void Open(const char* location);
  void Close();
  void Drop();
  void Flush();
  void Compact();

  void Set(const KeyType& key, const uint8_t* value, uint32_t size);
  bool Get(const KeyType& key, std::string* value);
  void Delete(const KeyType& key);

  uint64_t GetEntryCount() const {
    return original_index_.size() + inses_.size() - dels_.size();
  }
  uint64_t GetChangedCount() const {
    return inses_.size() + dels_.size();
  }
  uint64_t GetDataFileSize() const { return data_file_length_; }
  uint64_t GetDataBytes() const { return actual_bytes_; }

  std::string GetPath() const {
    return path_;
  }

 private:
  struct BlockInfo {
    uint32_t size;
    uint64_t position;
  };
  bool ReadBlockInfo(const BlockInfo& blk,
    std::string* value);
  std::string path_;
  int fd_;  // data.cab fd, use along with buffer.
  uint64_t data_file_length_;
  uint64_t actual_bytes_;
  typedef __gnu_cxx::hash_map<KeyType, BlockInfo, KeyHashFunc,
    std::equal_to<KeyType>, __gnu_cxx::__pool_alloc<BlockInfo> > MapType;
  typedef __gnu_cxx::hash_set<KeyType, KeyHashFunc,
    std::equal_to<KeyType>, __gnu_cxx::__pool_alloc<KeyType> > SetType;
  MapType original_index_;
  MapType inses_;
  SetType dels_;
  std::vector<uint8_t> buf_;
  uint32_t buf_pos_;
};
}  // namespace cabinet

#endif  // CABINET_CABINET_H_
