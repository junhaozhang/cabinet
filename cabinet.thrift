/**
 * Copyright 2013 i-MD.com. All rights reserved.
 *
 * Cabinet Storage Service Definition.
 *
 * @author: junhao.zhang@i-md.com
*/
namespace java com.imd.cabinet
namespace cpp cabinet

enum DbType {
  INT32,
  INT64,
  STRING
}

struct DbMeta {
  1: bool compressed;
  2: DbType type;
}

struct DbInfo {
  1: DbMeta meta;
  2: i64 entryCount;
  3: i64 dataBytes;
  4: i64 dataFileSize;
}

struct ServerInfo {
  1: i32 connections;
  2: map<string, DbInfo> dbs;
}

struct KeyType {
  1: optional i32 intKey;
  2: optional i64 longKey;
  3: optional string strKey;
}

struct GetInfo {
  1: bool got;
  2: string value;
}

exception BadDbName{}
exception DbExists{}
exception DbNotExist{}
exception IOException{}

service CabinetStorageService {
  string Ping(),
  ServerInfo GetServerInfo(),

  void Create(1: string dbName, 2: DbMeta meta) throws (1: BadDbName badDbName, 2: DbExists dbExists, 3: IOException ioException),
  void Drop(1: string dbName) throws (1: BadDbName badDbName, 2: DbNotExist dbExists, 3: IOException ioException),
  DbInfo GetDbInfo(1: string dbName) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist),
  void Compact(1: string dbName) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist),

  GetInfo Get(1: string dbName, 2: KeyType key) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),
  void Set(1: string dbName, 2: KeyType key, 3: binary value) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),
  void Delete(1: string dbName, 2: KeyType key) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),
  void Flush(1: string dbName) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),

  list<GetInfo> BatchGet(1: string dbName, 2: list<KeyType> keys) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),
  void BatchSet(1: string dbName, 2: list<KeyType> keys, 3: list<binary> values) throws (1: BadDbName badDbName, 2: DbNotExist dbNotExist, 3: IOException ioException),
}
