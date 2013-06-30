/**
 * Copyright 2013 i-MD.com. All Rights Reserved.
 *
 * Cabinet Exceptions.
 *
 * @author junhao.zhang@i-md.com (Bryan Zhang)
*/

#ifndef CABINET_EXCEPTIONS_H_
#define CABINET_EXCEPTIONS_H_

namespace cabinet {
// exceptions.
class CabinetException : public std::exception {
  public:
    CabinetException(
      const char* type,
      const char* filename,
      int lineno,
      int err,
      const char* errstr) : type_(type), filename_(filename), lineno_(lineno), err_(err), errstr_(errstr) {}
    std::string GetType() const { return type_; }
    std::string GetFileName() const { return filename_; }
    int GetErrno() const { return err_; }
    std::string GetErrStr() const { return errstr_; }
    virtual const char* what() const throw() {
      std::ostringstream oss;
      oss << type_ << "\t" << filename_ << "\t" << lineno_ << "\t" << err_ << "\t" << errstr_;
      return oss.str().c_str();
    }
    virtual ~CabinetException() throw() {}
  private:
    std::string type_;
    std::string filename_;
    int lineno_;
    int err_;
    std::string errstr_;
};

class OpenFileException : public CabinetException {
 public:
   OpenFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Open", filename, lineno, err, errstr) {}
};

class ReadFileException : public CabinetException {
 public:
   ReadFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Read", filename, lineno, err, errstr) {}
};

class WriteFileException : public CabinetException {
 public:
   WriteFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Write", filename, lineno, err, errstr) {}
};

class StatFileException : public CabinetException {
 public:
   StatFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Stat", filename, lineno, err, errstr) {}
};

class SeekFileException : public CabinetException {
 public:
   SeekFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Seek", filename, lineno, err, errstr) {}
};

class TruncateFileException : public CabinetException {
 public:
  TruncateFileException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("Truncate", filename, lineno, err, errstr) {}
};

class FileCorruptException : public CabinetException {
 public:
   FileCorruptException(const char* filename, int lineno, int err, const char* errstr) : CabinetException("FileCorrupt", filename, lineno, err, errstr) {}
};
}  // namespace cabinet

#endif  // CABINET_EXCEPTIONS_H_
