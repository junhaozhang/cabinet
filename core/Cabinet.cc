/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Storage Implementation
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#include "cabinet/Cabinet.h"

#include <endian.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <hash_map>
#include <hash_set>
#include <queue>

using std::priority_queue;
using std::string;

namespace {
struct IndexEntry {
  uint32_t key;
  uint32_t size;
  uint64_t position;
  bool operator < (const IndexEntry& rhs) const {
    return position < rhs.position;
  }
};

struct FreeFileData {
  uint64_t free_block_bytes_count;
  uint32_t free_block_count;
};

static const uint32_t sBufferSize = 4 * 1024 * 1024;
static const uint64_t sInvalidPosition = 0xffffffffffffffff;
static const uint32_t sInvalidSize = 0xffffffff;
}  // namespace

namespace cabinet {
Cabinet::Cabinet() : fd_(-1),
                     data_file_length_(0), dbgfd_(-1), err_(E_SUCCESS),
                     free_block_count_(0), free_block_bytes_count_(0) ,
                     buf_pos_(0) {
  buf_.resize(sBufferSize);
}

Cabinet::Cabinet(const char* file_name) : fd_(-1),
                  data_file_length_(0), dbgfd_(-1), err_(E_SUCCESS),
                  free_block_count_(0), free_block_bytes_count_(0),
                  buf_pos_(0) {
  buf_.resize(sBufferSize);
  Open(file_name);
}

Cabinet::~Cabinet() {
  Close();
}

bool Cabinet::Open(const char* location) {
  Close();
  if (err_ != E_SUCCESS) {
    return false;
  }

  // normalize path
  path_ = location;
  if ((*path_.rend()) != '/') {
    path_ += '/';
  }

  // mkdir
  mkdir(path_.c_str(), S_IRUSR | S_IWUSR | S_IEXEC);

  // open data file
  // create the file if not exists
  fd_ = open((path_ + "data.cab").c_str(),
    O_RDWR | O_CREAT | O_NONBLOCK, S_IRUSR | S_IWUSR);
  if (fd_ == -1) {
    SetError(E_OPEN, __FILE__, __LINE__);
    return false;
  }

  // stat and init file length
  struct stat f_stat;
  if (lstat((path_ + "data.cab").c_str(), &f_stat) == -1) {
    SetError(E_STAT, __FILE__, __LINE__);
    close(fd_);
    fd_ = -1;
    return false;
  }
  data_file_length_ = f_stat.st_size;

  // read index from the indexing file
  // create the file if not exists
  FILE* file = fopen((path_ + "index").c_str(), "a");
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    close(fd_);
    fd_ = -1;
    return false;
  }
  fclose(file);

  file = fopen((path_ + "index").c_str(), "r");
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    close(fd_);
    fd_ = -1;
    return false;
  }
  IndexEntry index_entry;
  while (fread(&index_entry, sizeof(index_entry), 1, file) == 1) {
    index_entry.key = le32toh(index_entry.key);
    index_entry.position = le64toh(index_entry.position);
    index_entry.size = le32toh(index_entry.size);

    // if deleted from original index
    if (index_entry.position == sInvalidPosition &&
      index_entry.size == sInvalidSize) {
      MapType::iterator itr = original_index_.find(index_entry.key);
      if (itr != original_index_.end()) {
        original_index_.erase(itr);
      }
    } else {
      BlockInfo& info = original_index_[index_entry.key];
      info.position = index_entry.position;
      info.size = index_entry.size;
    }
  }
  fclose(file);

  // read free block count, free block bytes count
  // create the file if not exists
  file = fopen((path_ + "free").c_str(), "a");
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    close(fd_);
    fd_ = -1;
    return false;
  }
  fclose(file);

  file = fopen((path_ + "free").c_str(), "r");  // creat if not exists
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    close(fd_);
    fd_ = -1;
    return false;
  }
  FreeFileData data;
  if (fread(&data, sizeof(data), 1, file) == 1) {
    free_block_count_ = le32toh(data.free_block_count);
    free_block_bytes_count_ = le64toh(data.free_block_bytes_count);
  }
  fclose(file);
  return true;
}

void Cabinet::Close() {
  if (fd_ == -1) {
    return;
  }

  if (!Flush()) {
    return;
  }

  // reset to initial state
  close(fd_);
  fd_ = -1;
  data_file_length_ = 0;

  original_index_.clear();
  free_block_count_ = 0;
  free_block_bytes_count_ = 0;

  path_.clear();
}

bool Cabinet::Set(uint32_t key, const uint8_t* value, uint32_t size) {
  // firstly remove old data
  Delete(key);

  // write data into buffer
  if (buf_pos_ + size > buf_.size()) {
    if (!Flush()) {
      return false;
    }
  }
  if (size > buf_.size()) {
    ssize_t ret = pwrite(fd_, value, size, data_file_length_);
    if (ret != size) {
      SetError(E_WRITE, __FILE__, __LINE__);
      return false;
    }
    data_file_length_ += size;
    SetType::iterator itr = dels_.find(key);
    if (itr != dels_.end()) {
      dels_.erase(itr);
    }
    BlockInfo& blk = inses_[key];
    blk.position = data_file_length_;
    blk.size = size;
    return Flush();
  }

  memcpy(&buf_[buf_pos_], value, size);
  buf_pos_ += size;
  SetType::iterator itr = dels_.find(key);
  if (itr != dels_.end()) {
    dels_.erase(itr);
  }
  BlockInfo& blk = inses_[key];
  blk.position = data_file_length_ + buf_pos_ - size;
  blk.size = size;

  return true;
}

bool Cabinet::Get(uint32_t key, std::string* value, ErrorCode* ecode) {
  // finding in insert map
  MapType::iterator itr = inses_.find(key);
  if (itr != inses_.end()) {
    return ReadBlockInfo(itr->second, value, ecode);
  }

  // finding in delete set
  SetType::iterator itr_set = dels_.find(key);
  if (itr_set != dels_.end()) {
    return false;
  }

  // finding in original index map
  itr = original_index_.find(key);
  if (itr != original_index_.end()) {
    return ReadBlockInfo(itr->second, value, ecode);
  }

  return false;
}

void Cabinet::Delete(uint32_t key) {
  uint32_t size = 0;
  MapType::iterator itr = inses_.find(key);
  if (itr != inses_.end()) {
    size = itr->second.size;
    inses_.erase(itr);
    dels_.insert(key);
  } else if (dels_.find(key) != dels_.end()) {
    return;
  } else if ((itr = original_index_.find(key)) != original_index_.end()) {
    size = itr->second.size;
    dels_.insert(key);
  } else {
    return;
  }

  // update free block info
  ++free_block_count_;
  free_block_bytes_count_ += size;
}

bool Cabinet::CopyFrom(Cabinet* cab) {
  if (cab->fd_ == -1) {
    SetError(E_NOTOPEN, __FILE__, __LINE__);
    return false;
  }

  string path = path_;
  Close();
  path_ = path;

  // remove old data
  string cmdline = "rm -rf ";
  cmdline += path_;
  system(cmdline.c_str());

  // open file handles
  if (!Open(path_.c_str())) {
    return false;
  }

  // insert all index entries into a priority queue
  cab->Flush();
  priority_queue<IndexEntry> entry_que;
  IndexEntry entry;
  for (MapType::const_iterator itr = cab->original_index_.begin();
       itr != cab->original_index_.end(); ++itr) {
    entry.key = itr->first;
    entry.position = itr->second.position;
    entry.size = itr->second.size;
    entry_que.push(entry);
  }

  // getting and writing
  string value;
  while (!entry_que.empty()) {
    entry = entry_que.top();
    entry_que.pop();
    if (cab->Get(entry.key, &value)) {
      if (!Set(entry.key,
        reinterpret_cast<const uint8_t*>(value.c_str()), value.size())) {
        return false;
      };
    }
  }

  // flushing
  return Flush();
}

bool Cabinet::Flush() {
  if (buf_pos_ == 0 && inses_.empty() && dels_.empty()) {
    return true;
  }

  // mkdir
  mkdir(path_.c_str(), S_IRUSR | S_IWUSR | S_IEXEC);

  // writing buffer into data file
  if (buf_pos_ != 0) {
    ssize_t ret = pwrite(fd_, &buf_[0], buf_pos_, data_file_length_);
    if (ret != buf_pos_) {
      SetError(E_WRITE, __FILE__, __LINE__);
      return false;
    }
    data_file_length_ += buf_pos_;
    buf_pos_ = 0;
  }

  // appending entries from inses_ & dels_
  FILE* file = fopen((path_ + "index").c_str(), "r+b");
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    return false;
  }
  // in case the last entry in the file corrupts.
  struct stat st;
  if (lstat((path_ + "index").c_str(), &st) == -1) {
    SetError(E_STAT, __FILE__, __LINE__);
    fclose(file);
    return false;
  }
  if (fseek(file,
      st.st_size / sizeof(IndexEntry) * sizeof(IndexEntry), SEEK_SET) != 0) {
    SetError(E_SEEK, __FILE__, __LINE__);
    fclose(file);
    return false;
  }

  IndexEntry index_entry;
  for (MapType::iterator itr = inses_.begin();
      itr != inses_.end(); ++itr) {
    index_entry.key = htole32(itr->first);
    index_entry.position = htole64(itr->second.position);
    index_entry.size = htole32(itr->second.size);
    if (fwrite(&index_entry, sizeof(index_entry), 1, file) != 1) {
      SetError(E_WRITE, __FILE__, __LINE__);
      fclose(file);
      return false;
    }
  }
  for (MapType::iterator itr = inses_.begin();
      itr != inses_.end(); ++itr) {
    original_index_[itr->first] = itr->second;
  }
  inses_.clear();
  for (SetType::iterator itr = dels_.begin();
      itr != dels_.end(); ++itr) {
    index_entry.key = htole32(*itr);
    index_entry.position = sInvalidPosition;
    index_entry.size = sInvalidSize;
    if (fwrite(&index_entry, sizeof(index_entry), 1, file) != 1) {
      SetError(E_WRITE, __FILE__, __LINE__);
      fclose(file);
      return false;
    }
  }
  MapType::iterator itr_map;
  for (SetType::iterator itr = dels_.begin();
      itr != dels_.end(); ++itr) {
    itr_map = original_index_.find(*itr);
    if (itr_map != original_index_.end()) {
      original_index_.erase(itr_map);
    }
  }
  dels_.clear();
  fclose(file);

  // writing free list info
  file = fopen((path_ + "free.tmp").c_str(), "w");
  if (!file) {
    SetError(E_OPEN, __FILE__, __LINE__);
    return false;
  }

  FreeFileData data;
  data.free_block_count = htole32(free_block_count_);
  data.free_block_bytes_count = htole64(free_block_bytes_count_);
  if (fwrite(&data, sizeof(data), 1, file) != 1) {
    SetError(E_WRITE, __FILE__, __LINE__);
    fclose(file);
    return false;
  }
  fclose(file);

  // rename
  rename((path_ + "free.tmp").c_str(), (path_ + "free").c_str());

  return true;
}

void Cabinet::SetError(ErrorCode ecode,
     const char* file_name, uint32_t lineno) {
  err_ = ecode;
  char buf[1024];
  int ret = snprintf(buf, sizeof(buf), "ERRCODE=%d,%s:%u %s(errno=%d)\n",
            ecode, file_name, lineno, sys_errlist[errno], errno);
  if (ret > 0) {
    write(dbgfd_, buf, ret);
  }
}

bool Cabinet::ReadBlockInfo(const BlockInfo& blk,
    std::string* value, ErrorCode* ecode) {
  value->clear();
  value->resize(blk.size);
  if (blk.size > 0) {
    if (blk.position < data_file_length_) {
      if (pread(fd_, &(*value)[0], blk.size, blk.position) != blk.size) {
        if (ecode) {
          *ecode = E_READ;
        }
        SetError(E_READ, __FILE__, __LINE__);
        return false;
      }
    } else {  // read from memory
      memcpy(&(*value)[0], &buf_[blk.position - data_file_length_], blk.size);
    }
  }

  if (ecode) {
    *ecode = E_SUCCESS;
  }
  return true;
}

}  // namespace cabinet
