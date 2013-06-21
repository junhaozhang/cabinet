/**
 * Copyright 2013 i-MD.com. All rights reserved.
 *
 * Cabinet Storage Service Definition.
 *
 * @author: junhao.zhang@i-md.com
*/

namespace java com.imd.cabinet
namespace cpp cabinet

struct DbInfo {
  3: i64 dataFileSize;
};

struct ServerInfo {
  1: i32 connections;
  2: map<string, DbInfo> dbs;
};

enum DBType {
  1: INT32,
  2: INT64,
  3: STRING
};

struct DbMeta {
  1: bool compressed = false;
  2: DBType type = DBType.INT32;
};

struct DbInfo {
  1: DbMeta meta;
  2: i64 entryCount;
  3: i64 indexBytes;
  4: i64 indexFileSize;
  5: i64 dataBytes;
  6: i64 dataFileSize;
};

struct TKeyType {
  1: optional i32 intKey;
  2: optional i64 longKey;
  3: optional string strKey;
};

struct GetInfo {
  1: required bool got;
  2: optional value;
};

exception BadDbName{};
exception DbExists{};
exception DbNotExist{};
exception IOException{};
service CabinetStroageService {
  string Ping(),
  ServerInfo ServerInfo();

  void Create(1: string dbName, 2: DbMeta meta) throws BadDbName, DbExists,IOException;
  bool Drop(1: string dbName) throws BadDbName, DbNotExist, IOException;
  DbInfo GetDbInfo(1: string dbName) throws BadDbName, DbNotExist;
  void Compact(1: string dbName) throws BadDbName, DbNotExist;

  GetInfo Get(1: string dbName, 2: TKeyType key) throws BadDbName, DbNotExist, IOException;
  void Set(1: string dbName, 2: TKeyType key, 3: binary value) throws BadDbName, DbNotExist, IOException,
  void Delete(1: string dbName, 2: TKeyType key) throws BadDbName, DbNotExist, IOException;
  void Flush(1: string dbName) throws BadDbName, DbNotExist, IOException;

  vector<GetInfo> BatchGet(1: string dbName, 2: vector<TKeyType> keys) throws BadDbName, DbNotExist, IOException;
  void BatchSet(1: string dbName, 2: map<TKeyType, binary> entries) throws BadDbName, DbNotExist, IOException;
};
