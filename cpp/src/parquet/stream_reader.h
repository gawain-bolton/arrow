#ifndef PARQUET_STREAM_READER_H
#define PARQUET_STREAM_READER_H

#include "parquet/api/reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <cstring>
#include <vector>

namespace parquet {

class PARQUET_EXPORT StreamReader
{
public:
  explicit StreamReader(std::unique_ptr<ParquetFileReader>& File_Reader);

  ~StreamReader() = default;

  bool eof() const { return eof_; }

  // Moving is possible.
  StreamReader(StreamReader&&) noexcept = default;
  StreamReader& operator=(StreamReader&&) noexcept = default;

  // Copying is not allowed.
  StreamReader(const StreamReader&) = delete;
  StreamReader& operator=(const StreamReader&) = delete;

  StreamReader& operator>>(bool& v);

  StreamReader& operator>>(std::int32_t& v);

  StreamReader& operator>>(std::uint32_t& v);

  StreamReader& operator>>(std::int64_t& v);

  StreamReader& operator>>(std::uint64_t& v);

  StreamReader& operator>>(float& v);

  StreamReader& operator>>(double& v);

  StreamReader& operator>>(char& v);

  template <int N>
  StreamReader& operator>>(char (&v)[N])
  {
      return readFixedLength_(v,N);
  }

  // N.B. Cannot allow for reading to a arbitrary char pointer as the
  //      length cannot be verified.  It would also overshadow the
  //      char[N] input operator.
  //StreamReader& operator>>(char * v);

  StreamReader& operator>>(std::string& v);

  // Advance to next row.
  //
  void nextRow();

protected:
  template<typename ReaderType,
           typename T>
  StreamReader& read_(T& v)
  {
      auto * const reader{ static_cast<ReaderType *>(column_readers_[column_index_++].get()) };

      std::int64_t values_read;

      reader->ReadBatch(1,
                        nullptr,
                        nullptr,
                        &v,
                        &values_read);

      return *this;
  }

  StreamReader& readFixedLength_(char *    ptr,
                                 const int len);

  void read_(ByteArray& v);

  void read_(FixedLenByteArray& v);

  void nextRowGroup_();

  void checkColumn_(const Type::type        physical_type,
                    const LogicalType::type logical_type,
                    const int               length = 0);

private:
  using node_ptr_type = std::shared_ptr<schema::PrimitiveNode>;

  std::unique_ptr<ParquetFileReader> file_reader_;
  std::shared_ptr<FileMetaData> file_metadata_;
  std::shared_ptr<RowGroupReader> row_group_reader_;
  std::vector<std::shared_ptr<ColumnReader>> column_readers_;
  std::vector<node_ptr_type> nodes_;

  bool eof_{false};
  int row_group_index_{0};
  int column_index_{0};
};

}  // namespace parquet

#endif // PARQUET_STREAM_READER_H
