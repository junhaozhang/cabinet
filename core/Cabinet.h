/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Storage Header
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#ifndef CPP_CABINET_CABINET_H_
#define CPP_CABINET_CABINET_H_

#include <ext/pool_allocator.h>
#include <stdint.h>
#include <hash_map>
#include <hash_set>
#include <functional>
#include <string>
#include <vector>

namespace cabinet {
class Cabinet {
 public:
  enum ErrorCode {
    E_SUCCESS = 0,
    E_OPEN = 1,
    E_READ = 2,
    E_WRITE = 3,
    E_NOTOPEN = 4,
    E_STAT = 5,
    E_SEEK = 6,
  };

  Cabinet();
  explicit Cabinet(const char* location);
  virtual ~Cabinet();

  bool Open(const char* location);
  void Close();

  bool Set(uint32_t key, const uint8_t* value, uint32_t size);
  bool Get(uint32_t key, std::string* value, ErrorCode* ecode = NULL);
  void Delete(uint32_t key);
  bool Flush();
  void IgnoreError() {
    err_ = E_SUCCESS;
  }

  uint64_t GetRecordCount() const {
    return original_index_.size() + inses_.size() - dels_.size();
  }
  uint64_t GetChangedCount() const {
    return inses_.size() + dels_.size();
  }
  uint64_t FreeBlockCount() const {
    return free_block_count_;
  }
  uint64_t FreeBlockBytesCount() const {
    return free_block_bytes_count_;
  }
  std::string GetPath() const {
    return path_;
  }

  ErrorCode GetErrorCode() const {
    return err_;
  }
  void SetDebugFD(int fd) {
    // debug fd is the file descriptor of error output
    dbgfd_ = fd;
  }
  bool CopyFrom(Cabinet* cab);

 private:
  struct BlockInfo {
    uint32_t size;
    uint64_t position;
  };
  bool ReadBlockInfo(const BlockInfo& blk,
    std::string* value, ErrorCode* ecode);
  void SetError(ErrorCode ecode, const char* file_name, uint32_t lineno);
  std::string path_;
  int fd_;
  uint64_t data_file_length_;
  int dbgfd_;
  ErrorCode err_;
  uint64_t free_block_count_;
  uint64_t free_block_bytes_count_;
  typedef __gnu_cxx::hash_map<uint32_t, BlockInfo, __gnu_cxx::hash<uint32_t>,
    std::equal_to<uint32_t>, __gnu_cxx::__pool_alloc<BlockInfo> > MapType;
  typedef __gnu_cxx::hash_set<uint32_t, __gnu_cxx::hash<uint32_t>,
    std::equal_to<uint32_t>, __gnu_cxx::__pool_alloc<uint32_t> > SetType;
  MapType original_index_;
  MapType inses_;
  SetType dels_;
  std::vector<uint8_t> buf_;
  uint32_t buf_pos_;
};
}  // namespace cabinet

#endif  // CPP_CABINET_CABINET_H_
