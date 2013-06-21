/**
 * Copyright 2012 i-MD.com. All Rights Reserved.
 *
 * Cabinet Thrift Server
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#include <Thrift.h>
#include <concurrency/PosixThreadFactory.h>
#include <protocol/TCompactProtocol.h>
#include <server/TThreadPoolServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <transport/TTransportException.h>
#include <signal.h>

#include <map>
#include <string>
#include "gen-cpp/CabinetStorage.h"
#include "cabinet/SyncCabinet.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

using ::apache::thrift::concurrency::PosixThreadFactory;
using ::apache::thrift::concurrency::ThreadFactory;
using ::apache::thrift::concurrency::ThreadManager;
using ::apache::thrift::concurrency::Thread;
using ::apache::thrift::concurrency::Runnable;
using ::apache::thrift::transport::TServerSocket;
using ::apache::thrift::transport::TSocket;
using ::apache::thrift::transport::TTransport;
using ::apache::thrift::transport::TServerTransport;
using ::apache::thrift::transport::TTransportException;
using ::apache::thrift::transport::TTransportFactory;
using ::apache::thrift::transport::TBufferedTransport;
using ::apache::thrift::transport::TBufferedTransportFactory;
using ::apache::thrift::TProcessor;
using ::apache::thrift::GlobalOutput;
using ::apache::thrift::TException;
using ::apache::thrift::server::TThreadPoolServer;
using ::apache::thrift::protocol::TProtocolFactory;
using ::apache::thrift::protocol::TProtocol;
using ::apache::thrift::protocol::TCompactProtocol;
using ::apache::thrift::protocol::TCompactProtocolFactory;

using std::map;
using std::string;
using std::vector;
using boost::shared_ptr;

using cabinet::Cabinet;
using cabinet::SyncCabinet;
using cabinet::CabinetStorageClient;
using cabinet::CabinetStorageIf;
using cabinet::CabinetStorageProcessor;
using cabinet::GetInfo;
using cabinet::BadName;
using cabinet::NotOpenYet;
using cabinet::IOError;
using cabinet::InvalidParam;

DEFINE_string(data_root, "/data/cabinet/", "cabinet data root path.");
DEFINE_string(daemon, false, "Fork and detach.");
DEFINE_string(address, "localhost", "specify bind address.");
DEFINE_int32(port, 9090, "cabinet server bind port.");
DEFINE_int32(flushinterval, 10,
    "flush & fsync db if time past this interval since last flush time.");

class FlushThread {
};

struct SyncCabinet {
  shared_ptr<CabinetTag> ptr;
  shared_ptr<::apache::thrift::concurrency::ReadWriteMutex> rwmutex_;
};

class CabinetStorageHandler : virtual public CabinetStorageIf {
 public:
  CabinetStorageHandler() {}

  virtual ~CabinetStorageHandler() {
    for (map<string, SyncCabinet*>::iterator itr = cabs_.begin();
        itr != cabs_.end(); ++itr) {
      delete itr->second;
    }
    cabs_.clear();
  }

  // NOTE: not thread safe, must use before serve()
  void SetDataPath(const char* data_path) {
    data_path_ = data_path;
    if ((*data_path_.rbegin()) != '/') {
      data_path_.push_back('/');
    }
    LOG(INFO) << "data path is " << data_path_ << "\n";

    requireFileLock(data_path_ + "cabinetd.lock"); 

    // Opening dbs.
    OpenDbs("*"); // should check dbname.
  }

  void Ping() {
    return "pong";
  }

  DbInfo _GetDbInfo(std::map<std::string, SyncCabinet>::iterator itr) {
    guard(itr->second.rlock());
    DbInfo info;
    info.meta.compressed = itr->second.ptr.IsCompressed();
    string type = itr->second.ptr.GetDbType();
    if (type == "INT32") {
      info.meta.type = DBType.INT32;
    } else if (type == "INT64") {
      info.meta.type = DBType.INT64;
    } else {
      info.meta.type = DBType.STRING;
    }
    info.entryCount = itr->second.ptr.GetEntryCount();
    info.indexBytes = itr->second.ptr.GetIndexBytes();
    info.indexFileSize = itr->second.ptr.GetIndexFileSize();
    info.dataBytes = itr->second.ptr.GetDataBytes();
    info.dataFileSize = itr->second.ptr.GetDataFileSize();
    return info;
  }

  ServerInfo ServerInfo() {
    ::apache::thrift::concurrency::RWGuard guard(
      rwmutex_, ::apache::thrift::concurrency::RW_READ);

    ServerInfo ret;
    for (std::map<string, SyncCabinet>::iterator itr = dbs_.begin(); itr != dbs_.end(); ++itr) {
      ret.dbs[itr->first] = _GetDbInfo(itr);
    }
    return ret;
  };

  void _CheckDbName(const std::string& dbName) {
    if (dbName.isEmpty() || dbName.contains('/')) {
      throw BadDbName;
    }
  };

  void Create(const std::string& dbName, const DbMeta& meta) {
     ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_WRITE);

    _CheckDbName(dbName);
    if (dbs_.find(dbName) != dbs_.end()) {
      throw DbExists();
    }
    SyncCabinet sync;
    try {
      sync->ptr.reset(new Cabinet(data_path_ + "/" + dbName));
      sync->rwLock.reset(new RWLock);
    } catch (exception& e) {
      LOG(INFO) << "Cabinet Open Exception: " + e.what();
      throw IOException;
    }
    dbs_[dbName] = sync;
  };

  Map<std::string, SyncCabinet>::iterator _GetSafeIterator(const std::string& dbName) {
    Map<std::string, SyncCabinet>::iterator itr = dbs_.find(dbName);
    if (itr == dbs_.end()) {
      throw DbNotExist();
    }
    return itr;
  }

  void Drop(const std::string& dbName) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_WRITE);

    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);

    // here no need to lock again.
    try {
      itr->second.ptr->Drop();
    } catch (exception& e) {
      LOG(INFO) << "Drop db " << dbName << " exception: " << e.what();
      throw IOException();
    }

    dbs_.erase(itr);
  };

  DbInfo GetDbInfo(const std::string& dbName) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);

    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    return _GetDbInfo(itr);
  }

  void Compact(const std::string& dbName) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->wlock);
    itr->second.ptr->Compact();
  }

  GetInfo Get(const std::string& dbName, TKeyType& key) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbname(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->rlock);
    GetInfo ret;
    try {
      if (itr->second.type == DBType.INT32) {
        ret.got = itr->second.ptr->Get(key.intKey, &ret.value);
      } else if (itr->second.type == DBType.INT64) {
        ret.got = itr->second.ptr->Get(key.longKey, &ret.value);
      } else {
        ret.got = itr->second.ptr->Get(key.strKey, &ret.value);
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception while Get(" << dbName << ", " << key << ": " + e.what();
      throw IOException();
    }
    return ret;
  }

  void Set(const std::string& dbName, const TKeyType& key, const std::string& value) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->wlock);
    try {
      if (itr->second.type == DBType.INT32) {
        itr->second.ptr->Set(key.intKey, &ret.value);
      } else if (itr->second.type == DBType.INT64) {
        itr->second.ptr->Set(key.longKey, &ret.value);
      } else {
        itr->second.ptr->Set(key.strKey, &ret.value);
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs while Set: " << e.what();
      throw IOException();
    }
    return ret;
  }

  void Delete(const std::string& dbName, const TKeyType& key) {
    ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->wlock);
    try {
      if (itr->second.type == DBType.INT32) {
        itr->second.ptr->Delete(key.intKey);
      } else if (itr->second.type == DBType.INT64) {
        itr->second.ptr->Delete(key.longKey);
      } else {
        itr->second.ptr->Delete(key.strKey);
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs while Delete: " << e.what();
      throw IOException();
    }
  }

  void Flush(const std::string& dbName) {
    ::apache::thrift::concurrency::RWGuard guard(
    rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->second.ptr.wlock);
    try {
      itr->second.ptr->Flush();
    } catch (exception& e) {
      LOG(INFO) << "Exeption occurs while Flush: " << e.what();
      throw IOException();
    }
  }

  std::vector<GetInfo> BatchGet(const std::string& dbName, const std::vector<TKeyType>& keys) {
       ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->rlock);
    GetInfo info;
    std::vector<GetInfo> ret;
    try {
      if (itr->second.type == DBType.INT32) {
        for (std::vector<TKeyType> i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = itr->second.ptr->Get(i->intKey, &info.value);
          ret.push(info);
        }
      } else if (itr->second.type == DBType.INT64) {
        for (std::vector<TKeyType> i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = itr->second.ptr->Get(i->longKey, &info.value);
          ret.push(info);
        }
      } else {
        for (std::vector<TKeyType> i = keys.begin(); i != keys.end(); ++i) {
          info.value = "";
          info.got = itr->second.ptr->Get(i->strkey, &info.value);
          ret.push(info);
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchGet: " << e.what();
      throw IOException();
    }
    return ret;
  }

  void BatchSet(const std::string& dbName, const std::map<TKeyType, std::string>& entries) {
       ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    Guard(itr->ptr.wlock);

    try {
      if (itr->second.type == DBType.INT32) {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Set(i->first.intKey, i->second);
        }
      } else if (itr->second.type == DBType.INT64) {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Set(i->first.longKey, i->second);
        }
      } else {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Set(itr->first.strKey, i->second);
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchSet: " << e.what();
      throw IOException();
    }
  }

  void BatchDelete(const std::string& dbName, const std::vector<TKeyType>& keys) {
       ::apache::thrift::concurrency::RWGuard guard(
       rwmutex_, ::apache::thrift::concurrency::RW_READ);
    _CheckDbName(dbName);
    Map<std::string, SyncCabinet>::iterator itr = _GetSafeIterator(dbName);
    try {
      if (itr->second.type == DBType.INT32) {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Delete(i->intKey);
        }
      } else if (itr->second.type == DBType.INT64) {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Delete(i->longKey);
        }
      } else {
        for (std::map<TKeyType, std::string>::iterator i = entries.begin(); i != entries.end(); ++i) {
          itr->second.ptr.Delete(itr->strKey);
        }
      }
    } catch (exception& e) {
      LOG(INFO) << "Exception occurs when BatchSet: " << e.what();
      throw IOException();
    }
  }

 private:
  map<string, SyncCabinet> cabs_;
  string data_path_;
};

TThreadPoolServer* g_server = NULL;

static void sig_handler(int sig) {
  LOG(INFO) << "Interrupt!signal = " << sig;
  g_server->stop();
  LOG(INFO) << "Server will stop immediately.";
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  openLogFile(FLAGS_log_path.c_str());
  if (FLAGS_daemon) {
    daemonize();
  }
  signal(SIGINT, sig_handler);

  // init server handler
  shared_ptr<CabinetStorageHandler> handler(new CabinetStorageHandler);
  handler->SetDataPath(FLAGS_data_root.c_str());

  // startup TNonblockingPoolServer
  shared_ptr<TProcessor> processor(new CabinetStorageProcessor(handler));
  shared_ptr<TProtocolFactory> protocolFactory(new TCompactProtocolFactory());

  // thread manager, thread number default set to 2*CPU
  shared_ptr<ThreadManager> threadManager =
    ThreadManager::newSimpleThreadManager();
  threadManager->threadFactory(threadFactory);
  threadManager->start();

  // server cron job.
  shared_ptr<CabinetServerCronJob> cron(new CabinetServerCronJob());

  TNonblockingServer server(processor, protocolFactory, FLAGS_address.c_str(), FLAGS_port, threadManager, cron);

  g_server = &server;
  server.serve();

  LOG(INFO) << "server stopped!";
  return 0;
}

