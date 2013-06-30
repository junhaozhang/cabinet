/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Storage Implementation
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#include "TCabinet.h"

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

#include "CabinetExceptions.h"

struct KeyData {
  void* ptr;
  uint32_t len;
};

namespace {
static const uint32_t sBufferSize = 4 * 1024 * 1024;
static const uint64_t sInvalidPosition = 0xffffffffffffffff;
static const uint32_t sInvalidSize = 0xffffffff;
}  // namespace

namespace cabinet {
using std::string;

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::TCabinet() : fd_(-1),
                     data_file_length_(0), actual_bytes_(0), buf_pos_(0) {
  buf_.resize(sBufferSize);
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::TCabinet(const char* file_name) : fd_(-1),
                  data_file_length_(0), actual_bytes_(0), buf_pos_(0) {
  buf_.resize(sBufferSize);
  Open(file_name);
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::~TCabinet() {
  Close();
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Open(const char* location) {
  Close();

  // normalize path
  path_ = location;
  if ((*path_.rend()) != '/') {
    path_ += '/';
  }

  // mkdir
  mkdir(path_.c_str(), S_IRUSR | S_IWUSR | S_IEXEC);

  // open data file
  // create the file if not exists
  fd_ = open((path_ + "data").c_str(),
    O_RDWR | O_CREAT | O_NONBLOCK, S_IRUSR | S_IWUSR);
  if (fd_ == -1) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }

  // stat and init file length
  struct stat f_stat;
  if (lstat((path_ + "data").c_str(), &f_stat) == -1) {
    throw StatFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  data_file_length_ = f_stat.st_size;

  // read index from the indexing file
  // create the file if not exists
  FILE* file = fopen((path_ + "index").c_str(), "a");
  if (!file) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  fclose(file);

  file = fopen((path_ + "index").c_str(), "r");
  if (!file) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }

  KeyType key;
  BlockInfo block;
  while (KeyReader()(file, key)) {
    if (fread(&block, sizeof(block), 1, file) != 1) {
      int err = errno;
      fclose(file);
      throw FileCorruptException(__FILE__, __LINE__, err, strerror(err));
    }

    block.position = le64toh(block.position);
    block.size = le32toh(block.size);

    // if deleted from original index
    if (block.position == sInvalidPosition &&
      block.size == sInvalidSize) {
      typename MapType::iterator itr = original_index_.find(key);
      if (itr != original_index_.end()) {
        original_index_.erase(itr);
        actual_bytes_ -= itr->second.size;
      }
    } else {
      if (original_index_.find(key) == original_index_.end()) {
        actual_bytes_ += block.size;
      }
      original_index_[key] = block;
    }
  }
  fclose(file);
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Close() {
  if (fd_ == -1) {
    return;
  }

  Flush();

  // reset to initial state
  close(fd_);
  fd_ = -1;
  data_file_length_ = 0;

  original_index_.clear();

  path_.clear();
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Drop() {
  Close();
  if (truncate((path_ + "data").c_str(), 0) != 0) {
    throw TruncateFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  if (truncate((path_ + "index").c_str(), 0) != 0) {
    throw TruncateFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  Open(path_.c_str());
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Set(const KeyType& key, const uint8_t* value, uint32_t size) {
  // firstly remove old data
  Delete(key);

  // write data into buffer
  if (buf_pos_ + size > buf_.size()) {
    Flush();
  }
  if (size > buf_.size()) {
    ssize_t ret = pwrite(fd_, value, size, data_file_length_);
    if (ret != size) {
      throw WriteFileException(__FILE__, __LINE__, errno, strerror(errno));
    }
    data_file_length_ += size;
    typename SetType::iterator itr = dels_.find(key);
    if (itr != dels_.end()) {
      dels_.erase(itr);
    }
    BlockInfo& blk = inses_[key];
    blk.position = data_file_length_;
    blk.size = size;
    Flush();
  }

  memcpy(&buf_[buf_pos_], value, size);
  buf_pos_ += size;
  typename SetType::iterator itr = dels_.find(key);
  if (itr != dels_.end()) {
    dels_.erase(itr);
  }
  BlockInfo& blk = inses_[key];
  blk.position = data_file_length_ + buf_pos_ - size;
  blk.size = size;
  actual_bytes_ += size;
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
bool TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Get(const KeyType& key, std::string* value) {
  // finding in insert map
  typename MapType::iterator itr = inses_.find(key);
  if (itr != inses_.end()) {
    return ReadBlockInfo(itr->second, value);
  }

  // finding in delete set
  typename SetType::iterator itr_set = dels_.find(key);
  if (itr_set != dels_.end()) {
    return false;
  }

  // finding in original index map
  itr = original_index_.find(key);
  if (itr != original_index_.end()) {
    return ReadBlockInfo(itr->second, value);
  }

  return false;
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Delete(const KeyType& key) {
  typename MapType::iterator itr = inses_.find(key);
  if (itr != inses_.end()) {
    inses_.erase(itr);
    dels_.insert(key);
    actual_bytes_ -= itr->second.size;
  } else if (dels_.find(key) == dels_.end() && (itr = original_index_.find(key)) != original_index_.end()) {
    original_index_.erase(itr);
    dels_.insert(key);
    actual_bytes_ -= itr->second.size;
  }
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Flush() {
  if (buf_pos_ == 0 && inses_.empty() && dels_.empty()) {
    return;
  }

  // mkdir
  mkdir(path_.c_str(), S_IRUSR | S_IWUSR | S_IEXEC);

  // writing buffer into data file
  bool needfsync = false;
  if (buf_pos_ != 0) {
    ssize_t ret = pwrite(fd_, &buf_[0], buf_pos_, data_file_length_);
    if (ret != buf_pos_) {
      throw WriteFileException(__FILE__, __LINE__, errno, strerror(errno));
    }
    needfsync = true;
    data_file_length_ += buf_pos_;
    buf_pos_ = 0;
  }

  // appending entries from inses_ & dels_
  FILE* file = fopen((path_ + "index").c_str(), "r+b");
  if (!file) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  // in case the last entry in the file corrupts.
  struct stat st;
  if (lstat((path_ + "index").c_str(), &st) == -1) {
    int err = errno;
    fclose(file);
    throw StatFileException(__FILE__, __LINE__, err, strerror(err));
  }

  BlockInfo block;
  for (typename MapType::iterator itr = inses_.begin();
      itr != inses_.end(); ++itr) {
    KeyWriter()(file, itr->first);
    block.position = htole64(itr->second.position);
    block.size = htole32(itr->second.size);
    if (fwrite(&block, sizeof(block), 1, file) != 1) {
      int err = errno;
      fclose(file);
      throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
    }
  }

  for (typename MapType::iterator itr = inses_.begin();
      itr != inses_.end(); ++itr) {
    original_index_[itr->first] = itr->second;
  }
  inses_.clear();

  for (typename SetType::iterator itr = dels_.begin();
      itr != dels_.end(); ++itr) {
    KeyWriter()(file, *itr);
    block.position = sInvalidPosition;
    block.size = sInvalidSize;
    if (fwrite(&block, sizeof(block), 1, file) != 1) {
      int err = errno;
      fclose(file);
      throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
    }
  }

  typename MapType::iterator itr_map;
  for (typename SetType::iterator itr = dels_.begin();
      itr != dels_.end(); ++itr) {
    itr_map = original_index_.find(*itr);
    if (itr_map != original_index_.end()) {
      original_index_.erase(itr_map);
    }
  }
  dels_.clear();
  fflush(file);
  fsync(fileno(file));
  fclose(file);
  if (needfsync) {
    fsync(fd_);
  }
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
void TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::Compact() {
  Flush();

  pid_t pid = getpid();
  std::stringstream oss;
  oss << path_ << "tmp-index." << pid;
  string tmpIndexPath = oss.str();
  FILE* tmpIndexFile = fopen(tmpIndexPath.c_str(), "wb");
  if (!tmpIndexFile) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }
  oss.clear();
  oss << path_ << "tmp-data." << pid;
  string tmpDataPath = oss.str();
  FILE* tmpDataFile = fopen(tmpDataPath.c_str(), "wb");
  if (!tmpDataFile) {
    int err = errno;
    fclose(tmpIndexFile);
    unlink(tmpIndexPath.c_str());
    throw OpenFileException(__FILE__, __LINE__, err, strerror(err));
  }

  MapType dupIndex;
  std::string value;
  uint64_t byte_count = 0;
  BlockInfo block;
  for (typename MapType::iterator itr = original_index_.begin(); itr != original_index_.end(); ++itr) {
    ReadBlockInfo(itr->second, &value);
    KeyWriter()(tmpDataFile, itr->first);
    block.position = htole64(itr->second.position);
    block.size = htole32(itr->second.size);
    if (fwrite(&block, sizeof(block), 1, tmpIndexFile) != 1 || fwrite(value.c_str(), value.size(), 1, tmpDataFile) != 1) {
      int err = errno;
      fclose(tmpIndexFile);
      unlink(tmpIndexPath.c_str());
      fclose(tmpDataFile);
      unlink(tmpIndexPath.c_str());
      throw WriteFileException(__FILE__, __LINE__, err, strerror(err));
    }
    block.position = byte_count;
    block.size = itr->second.size;
    dupIndex[itr->first] = block;
    byte_count += itr->second.size;
  }
  fflush(tmpIndexFile);
  fflush(tmpDataFile);
  fsync(fileno(tmpIndexFile));
  fsync(fileno(tmpDataFile));
  fclose(tmpIndexFile);
  fclose(tmpDataFile);

  close(fd_);
  fd_ = -1;
  rename(tmpIndexPath.c_str(), (path_ + "index").c_str());
  rename(tmpDataPath.c_str(), (path_ + "data").c_str());

  fd_ = open((path_ + "data").c_str(), O_RDWR | O_NONBLOCK);
  if (fd_ == -1) {
    throw OpenFileException(__FILE__, __LINE__, errno, strerror(errno));
  }

  original_index_.clear();
  original_index_ = dupIndex;
  actual_bytes_ = data_file_length_ = byte_count;
}

template <class KeyType, class KeyReader, class KeyWriter, class KeyHashFunc>
bool TCabinet<KeyType, KeyReader, KeyWriter, KeyHashFunc>::ReadBlockInfo(const BlockInfo& blk,
    std::string* value) {
  value->clear();
  value->resize(blk.size);
  if (blk.size > 0) {
    if (blk.position < data_file_length_) {
      if (pread(fd_, &(*value)[0], blk.size, blk.position) != blk.size) {
        throw ReadFileException(__FILE__, __LINE__, errno, strerror(errno));
        return false;
      }
    } else {  // read from memory
      memcpy(&(*value)[0], &buf_[blk.position - data_file_length_], blk.size);
    }
  }

  return true;
}

}  // namespace cabinet
