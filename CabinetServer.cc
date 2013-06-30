// TODO: daemonize, thrift iterator interface, merge KeyHeaderExtractor, KeyDataExtractor (no type in cabinet)
// 压缩
// compact实现,logfile, bind address, serverCron
// 在写错误的情况下，允许读，但是不允许写,ipython
// glob文件路径
// autotool
// 定时刷新、关闭超时cursor的线程
// 测试的点:
// 1)log文件,daemonize是否正常;
// 2)读写,compact, iterator

/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Thrift Server
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#include <signal.h>
#include <dirent.h>
#include <map>
#include <stdexcept>
#include <string>
#include <sys/file.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thrift/concurrency/Mutex.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportException.h>
#include "gen-cpp/CabinetStorageService.h"
#include "CabinetTypes.h"

using ::apache::thrift::concurrency::RWGuard;
using ::apache::thrift::concurrency::ReadWriteMutex;
using ::apache::thrift::concurrency::RW_WRITE;
using ::apache::thrift::concurrency::RW_READ;
using ::apache::thrift::concurrency::Runnable;
using ::apache::thrift::concurrency::PosixThreadFactory;
using ::apache::thrift::concurrency::ThreadFactory;
using ::apache::thrift::concurrency::ThreadManager;
using ::apache::thrift::concurrency::Thread;
using ::apache::thrift::server::TNonblockingServer;
using ::apache::thrift::transport::TServerSocket;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransport;
using ::apache::thrift::transport::TServerTransport;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::transport::TTransportFactory;
using ::apache::thrift::transport::TBufferedTransport;
using ::apache::thrift::transport::TBufferedTransportFactory;
using ::apache::thrift::TProcessor;
using ::apache::thrift::TException;
using ::apache::thrift::protocol::TProtocolFactory;
using ::apache::thrift::protocol::TProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::protocol::TCompactProtocolFactory;

using std::exception;
using std::runtime_error;
using std::map;
using std::string;
using std::stringstream;
using std::vector;
using boost::shared_ptr;

using cabinet::CabinetBase;
using cabinet::CabinetStorageServiceClient;
using cabinet::CabinetStorageServiceIf;
using cabinet::CabinetStorageServiceProcessor;
using cabinet::DbType;
using cabinet::DbInfo;
using cabinet::GetInfo;
using cabinet::ServerInfo;
using cabinet::DbMeta;
using cabinet::KeyType;
using cabinet::U32Cabinet;
using cabinet::U64Cabinet;
using cabinet::StringCabinet;
using cabinet::BadDbName;
using cabinet::DbExists;
using cabinet::DbNotExist;
using cabinet::IOException;

DEFINE_string(data_root, "/data/cabinet/", "cabinet data root path.");
DEFINE_string(log_path, "", "cabinet daemon log file.");
DEFINE_bool(daemon, false, "Fork and detach.");
DEFINE_string(address, "localhost", "specify bind address.");
DEFINE_int32(port, 9527, "cabinet server bind port.");
DEFINE_int32(flushinterval, 10,
    "flush & fsync db if time past this interval since last flush time.");

struct SyncCabinet {
  shared_ptr<CabinetBase> ptr;
  DbMeta meta;
  shared_ptr<ReadWriteMutex> rwmutex_;
};

class CabinetStorageHandler : virtual public CabinetStorageServiceIf {
 public:
  explicit CabinetStorageHandler(const char* data_path) : lockFile_(-1) {
    data_path_ = data_path;
    if ((*data_path_.rbegin()) != '/') {
      data_path_.push_back('/');
    }
    LOG(INFO) << "data path is " << data_path_ << "\n";

    _AcquirePathLock(); 

    // Opening dbs.
    _OpenDbs(); // should check dbname.
  }

  virtual ~CabinetStorageHandler() {
    _ReleasePathLock();
  }

  void Ping(string& _return) {
    _return = "pong";
  }

  void GetServerInfo(ServerInfo& ret) {
    RWGuard guard(rwmutex_, RW_READ);

    for (std::map<string, SyncCabinet>::iterator itr = dbs_.begin(); itr != dbs_.end(); ++itr) {
      ret.dbs[itr->first] = _GetDbInfo(itr);
    }
  };

  void Create(const std::string& dbName, const DbMeta& meta) {
     RWGuard guard(rwmutex_, RW_WRITE);

    _CheckDbName(dbName);
    if (dbs_.find(dbName) != dbs_.end()) {
      throw DbExists();
    }
    SyncCabinet sync;
    const char* path = (data_path_ + dbName).c_str();
    try {
      if (meta.type == DbType::INT32) {
        sync.ptr.reset(new U32Cabinet(path));
      } else if (meta.type == DbType::INT64) {
        sync.ptr.reset(new U64Cabinet(path));
      } else {  // DbType::STRING
        sync.ptr.reset(new StringCabinet(path));
      }
    } catch (exception& e) {
      LOG(INFO) << "Cabinet Open Exception: " << e.what();
      throw IOException();
    }
    sync.rwmutex_.reset(new ReadWriteMutex);
    dbs_[dbName] = sync;
  };

  void Drop(const std::string& dbName) {
    RWGuard guard(rwmutex_, RW_WRITE);

    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);

    // here no need to lock again.
    try {
      (itr->second.ptr.get())->Drop();
    } catch (exception& e) {
      LOG(INFO) << "Drop db " << dbName << " exception: " << e.what();
      throw IOException();
    }

    dbs_.erase(itr);
  };

  void GetDbInfo(DbInfo& ret, const std::string& dbName) {
    RWGuard guard(rwmutex_, RW_READ);

    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    ret = _GetDbInfo(itr);
  }

  void Compact(const std::string& dbName) {
    RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_WRITE);
    (itr->second.ptr.get())->Compact();
  }

  void Get(GetInfo& ret, const std::string& dbName, const KeyType& key) {
    RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_READ);
    try {
      if (itr->second.meta.type == DbType::INT32) {
        ret.got = ((U32Cabinet*)(itr->second.ptr.get()))->Get(key.intKey, &ret.value);
      } else if (itr->second.meta.type == DbType::INT64) {
        ret.got = ((U64Cabinet*)(itr->second.ptr.get()))->Get(key.longKey, &ret.value);
      } else {  // String
        ret.got = ((StringCabinet*)(itr->second.ptr.get()))->Get(key.strKey, &ret.value);
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception while Get(" << dbName << ", " << ": " << e.what();
      throw IOException();
    }
  }

  void Set(const std::string& dbName, const KeyType& key, const std::string& value) {
    RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_WRITE);
    try {
      if (itr->second.meta.type == DbType::INT32) {
        ((U32Cabinet*)(itr->second.ptr.get()))->Set(key.intKey, (const uint8_t*)value.c_str(), value.size());
      } else if (itr->second.meta.type == DbType::INT64) {
        ((U64Cabinet*)(itr->second.ptr.get()))->Set(key.longKey, (const uint8_t*)value.c_str(), value.size());
      } else {
        ((StringCabinet*)(itr->second.ptr.get()))->Set(key.strKey, (const uint8_t*)value.c_str(), value.size());
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs while Set: " << e.what();
      throw IOException();
    }
  }

  void Delete(const std::string& dbName, const KeyType& key) {
    RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_WRITE);
    try {
      if (itr->second.meta.type == DbType::INT32) {
        ((U32Cabinet*)(itr->second.ptr.get()))->Delete(key.intKey);
      } else if (itr->second.meta.type == DbType::INT64) {
        ((U64Cabinet*)(itr->second.ptr.get()))->Delete(key.longKey);
      } else {
        ((StringCabinet*)(itr->second.ptr.get()))->Delete(key.strKey);
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs while Delete: " << e.what();
      throw IOException();
    }
  }

  void Flush(const std::string& dbName) {
    RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_WRITE);
    try {
      (itr->second.ptr.get())->Flush();
    } catch (exception& e) {
      LOG(INFO) << "Exeption occurs while Flush: " << e.what();
      throw IOException();
    }
  }

  void BatchGet(std::vector<GetInfo>& ret, const std::string& dbName, const std::vector<KeyType>& keys) {
     RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_READ);
    GetInfo info;
    try {
      if (itr->second.meta.type == DbType::INT32) {
        U32Cabinet* cab = (U32Cabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = cab->Get(i->intKey, &info.value);
          ret.push_back(info);
        }
      } else if (itr->second.meta.type == DbType::INT64) {
        U64Cabinet* cab = (U64Cabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = cab->Get(i->longKey, &info.value);
          ret.push_back(info);
        }
      } else {
        StringCabinet* cab = (StringCabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = cab->Get(i->strKey.c_str(), &info.value);
          ret.push_back(info);
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchGet: " << e.what();
      throw IOException();
    }
  }

  void BatchSet(const std::string& dbName, const std::vector<KeyType>& keys, const std::vector<std::string>& values) {
     RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    RWGuard subGuard(*(itr->second.rwmutex_), RW_WRITE);

    std::vector<KeyType>::const_iterator i = keys.begin();
    std::vector<std::string>::const_iterator j = values.begin();
    try {
      if (itr->second.meta.type == DbType::INT32) {
        U32Cabinet* cab = ((U32Cabinet*)itr->second.ptr.get());
        for (; i != keys.end() && j != values.end(); ++i, ++j) {
          cab->Set(i->intKey, (const uint8_t*)(j->c_str()), j->size());
        }
      } else if (itr->second.meta.type == DbType::INT64) {
        U64Cabinet* cab = ((U64Cabinet*)itr->second.ptr.get());
        for (; i != keys.end() && j != values.end(); ++i, ++j) {
          cab->Set(i->longKey, (const uint8_t*)(j->c_str()), j->size());
        }
      } else {
        StringCabinet* cab = ((StringCabinet*)itr->second.ptr.get());
        for (; i != keys.end() && j != values.end(); ++i, ++j) {
          cab->Set(i->strKey, (const uint8_t*)(j->c_str()), j->size());
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchSet: " << e.what();
      throw IOException();
    }
  }

  void BatchDelete(const std::string& dbName, const std::vector<KeyType>& keys) {
     RWGuard guard(rwmutex_, RW_READ);
    _CheckDbName(dbName);
    map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    try {
      if (itr->second.meta.type == DbType::INT32) {
        U32Cabinet* cab = (U32Cabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          cab->Delete(i->intKey);
        }
      } else if (itr->second.meta.type == DbType::INT64) {
        U64Cabinet* cab = (U64Cabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          cab->Delete(i->longKey);
        }
      } else {
        StringCabinet* cab = (StringCabinet*)(itr->second.ptr.get());
        for (std::vector<KeyType>::const_iterator i = keys.begin(); i != keys.end(); ++i) {
          cab->Delete(i->strKey);
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchSet: " << e.what();
      throw IOException();
    }
  }

 private:
  map<std::string, SyncCabinet>::iterator _GetSafeIterator(const std::string& dbName) {
    map<std::string, SyncCabinet>::iterator itr = dbs_.find(dbName);
    if (itr == dbs_.end()) {
      throw DbNotExist();
    }
    return itr;
  }

  DbInfo _GetDbInfo(std::map<std::string, SyncCabinet>::iterator itr) {
    RWGuard guard(*itr->second.rwmutex_, RW_READ);
    DbInfo info;
    info.meta = itr->second.meta;
    CabinetBase* cab = itr->second.ptr.get();
    info.entryCount = cab->GetEntryCount();
    info.dataBytes = cab->GetDataBytes();
    info.dataFileSize = cab->GetDataFileSize();
    return info;
  }

  void _CheckDbName(const std::string& dbName) {
    if (dbName.empty() || dbName.find('/') != string::npos) {
      throw BadDbName();
    }
  };

  void _ReleasePathLock() {
    close(lockFile_);
    string name = data_path_ + "cabinetd.pid";
    unlink(name.c_str());
  }

  void _AcquirePathLock() {
    struct stat st;
    string name = data_path_ + "cabinetd.pid";

    if (access(name.c_str(), F_OK) == 0 && stat(name.c_str(), &st) == 0 && st.st_size > 0) {
      throw runtime_error("Absnormal last exit. Please check.");
    }

    lockFile_ = open(name.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    if( lockFile_ <= 0 ) {
      throw runtime_error("Open pid file failed!");
    }

    stringstream ss;
    ss << getpid() << std::endl;
    string s = ss.str();
    const char * data = s.c_str();
    if (write(lockFile_, data, strlen(data)) != strlen(data)) {
      close(lockFile_);
      throw std::runtime_error("Write pid failed!");
    }
    fsync(lockFile_);
  }

  void _OpenDbs() {
    DIR* dir = opendir(data_path_.c_str());
    struct dirent* entry = NULL;
    while ((entry = readdir(dir)) != NULL) {
      if (entry->d_type != DT_DIR || strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      _OpenDb(entry->d_name);
    }
    closedir(dir);
  }

  void _OpenDb(const char* dbname) {
    SyncCabinet cab;
    std::string dbPath = data_path_ + dbname;
    DbMeta meta = _GetDbMeta(dbname);
    if (meta.type == DbType::INT32) {
      cab.ptr.reset(new U32Cabinet(dbPath.c_str()));
    } else if (meta.type == DbType::INT64) {
      cab.ptr.reset(new U64Cabinet(dbPath.c_str()));
    } else {  // DbType.STRING
      cab.ptr.reset(new StringCabinet(dbPath.c_str()));
    }
    dbs_[dbname] = cab;
  }

  DbMeta _GetDbMeta(const char* dbname) {
    FILE* fp = fopen((data_path_ + dbname).c_str(), "rb");
    if (fp == NULL) {
      throw runtime_error("Db meta file missing!");
    }
    char buf[1024], type[1024];
    int compress = 0;
    size_t count = fread(buf, 1, sizeof(buf), fp);
    buf[count] = '\0';
    if (sscanf(buf, "%s%d", type, &compress) != 2) {
      throw runtime_error("Db meta file invalid!");
    }
    DbMeta ret;
    if (strcmp(type, "I32") == 0) {
      ret.type = DbType::INT32;
    } else if (strcmp(type, "I64") == 0) {
      ret.type = DbType::INT64;
    } else {
      ret.type = DbType::STRING;
    }
    ret.compressed = (compress != 0);
    fclose(fp);
  }

  map<string, SyncCabinet> dbs_;
  ReadWriteMutex rwmutex_;
  string data_path_;
  int lockFile_;
};

TNonblockingServer* g_server = NULL;

static void sig_handler(int sig) {
  LOG(INFO) << "Interrupt!signal = " << sig;
  g_server->stop();
  LOG(INFO) << "Server will stop immediately.";
}

static void daemonize() {
  if (fork() != 0) {
    exit(0);  // parent exits.
  }
  setsid(); // create a new session.
  int fd = -1;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    if (FLAGS_log_path.empty()) {
      dup2(fd, STDOUT_FILENO);
      dup2(fd, STDERR_FILENO);
    }
    if (fd > STDERR_FILENO) {
      close(fd);
    }
  }  
}

static void openLogFile() {
  if (FLAGS_log_path.empty()) {
    return;
  }

  int fd = open(FLAGS_log_path.c_str(), O_RDWR | O_CREAT | O_NONBLOCK, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    throw std::runtime_error("Open Log File Error!");
  }
  dup2(fd, STDOUT_FILENO);
  dup2(fd, STDERR_FILENO);
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  openLogFile();
  if (FLAGS_daemon) {
    daemonize();
  }
  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  signal(SIGKILL, sig_handler);

  // init server handler
  shared_ptr<CabinetStorageHandler> handler(new CabinetStorageHandler(FLAGS_data_root.c_str()));

  // startup TNonblockingServer
  shared_ptr<TProcessor> processor(new CabinetStorageServiceProcessor(handler));
  shared_ptr<TProtocolFactory> protocolFactory(new TCompactProtocolFactory());

  // thread manager, thread number default set to 2*CPU
  shared_ptr<ThreadManager> threadManager =
    ThreadManager::newSimpleThreadManager();
  shared_ptr<ThreadFactory> threadFactory =
    shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();

  TNonblockingServer server(processor, protocolFactory, FLAGS_port, threadManager);

  g_server = &server;
  server.serve();

  LOG(INFO) << "server stopped!";
  return 0;
}

