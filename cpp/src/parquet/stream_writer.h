#ifndef PARQUET_STREAM_WRITER_H
#define PARQUET_STREAM_WRITER_H

#include "parquet/api/writer.h"

#include <cstdint>
#include <memory>
#include <string>
#include <chrono>
#include <vector>

namespace parquet {

class PARQUET_EXPORT StreamWriter
{
public:
  explicit StreamWriter(std::unique_ptr<ParquetFileWriter>& File_Writer);

  ~StreamWriter();

  static void SetDefaultMaxRowGroupSize(const std::int64_t max_size);

  void SetMaxRowGroupSize(const std::int64_t max_size);

  // Moving is possible.
  StreamWriter(StreamWriter&&) noexcept = default;
  StreamWriter& operator=(StreamWriter&&) noexcept = default;

  // Copying is not allowed.
  StreamWriter(const StreamWriter&) = delete;
  StreamWriter& operator=(const StreamWriter&) = delete;

  StreamWriter& operator<<(const bool v);

  StreamWriter& operator<<(const std::int32_t v);

  StreamWriter& operator<<(const std::uint32_t v);

  StreamWriter& operator<<(const std::int64_t v);

  StreamWriter& operator<<(const std::uint64_t v);

  StreamWriter& operator<<(const std::chrono::milliseconds& v);

  StreamWriter& operator<<(const std::chrono::microseconds& v);

  StreamWriter& operator<<(const float v);

  StreamWriter& operator<<(const double v);

  StreamWriter& operator<<(const char v);

  template <int N>
  StreamWriter& operator<<(const char (&v)[N])
  {
      return writeFixedLength_(v,N);
  }

  StreamWriter& operator<<(const char * v);

  StreamWriter& operator<<(const std::string& v);

  // Advance to next row.
  //
  void nextRow();

  // Create a new row group.
  //
  void newRowGroup();

protected:
  template<typename WriterType,
           typename T>
  StreamWriter& write_(const T v)
  {
      auto * const writer{ static_cast<WriterType *>(row_group_writer_->column(column_index_++)) };

      writer->WriteBatch(1,
                         nullptr,
                         nullptr,
                         &v);

      if ( max_row_group_size_ > 0 )
      {
          row_group_size_ += writer->EstimatedBufferedValueBytes();
      }
      return *this;
  }

  StreamWriter& writeVariableLength_(const char *      data_ptr,
                                     const std::size_t data_len);

  StreamWriter& writeFixedLength_(const char *      data_ptr,
                                  const std::size_t data_len);

  void closeRowGroup_();

  void checkColumn_(const Type::type        physical_type,
                    const LogicalType::type logical_type,
                    const int               length = -1);

private:
  using node_ptr_type = std::shared_ptr<schema::PrimitiveNode>;

  struct null_deleter { void operator()(void *) {} };

  std::int64_t row_group_size_{0};
  std::int64_t max_row_group_size_{default_row_group_size_};
  std::int32_t column_index_{0};
  std::unique_ptr<ParquetFileWriter> file_writer_;
  std::unique_ptr<RowGroupWriter,null_deleter> row_group_writer_;
  std::vector<node_ptr_type> nodes_;

  static std::int64_t default_row_group_size_;
};

}  // namespace parquet

#endif // PARQUET_STREAM_WRITER_H
