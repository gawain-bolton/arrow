// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PARQUET_STREAM_READER_H
#define PARQUET_STREAM_READER_H

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/optional.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/stream_writer.h"

namespace parquet {

/// \brief A class for reading Parquet files using an output stream type API.
///
/// The values given must be of the correct type i.e. the type must
/// match the file schema exactly otherwise a ParquetException will be
/// thrown.
///
/// The user must explicitly advance to the next row using the
/// EndRow() function or EndRow input manipulator.
///
/// Currently there is no support for repeated fields.
///
class PARQUET_EXPORT StreamReader {
 public:
  template <typename T>
  using optional = arrow::util::optional<T>;

  // N.B. Default constructed objects are not usable.  This
  //      constructor is provided so that the object may be move
  //      assigned afterwards.
  StreamReader() = default;

  explicit StreamReader(std::unique_ptr<ParquetFileReader> reader);

  ~StreamReader() = default;

  bool eof() const { return eof_; }

  int current_column() const { return column_index_; }

  int64_t current_row() const { return current_row_; }

  int num_columns() const;

  int64_t num_rows() const;

  // Moving is possible.
  StreamReader(StreamReader&&) = default;
  StreamReader& operator=(StreamReader&&) = default;

  // Copying is not allowed.
  StreamReader(const StreamReader&) = delete;
  StreamReader& operator=(const StreamReader&) = delete;

  StreamReader& operator>>(bool& v);

  StreamReader& operator>>(int8_t& v);

  StreamReader& operator>>(uint8_t& v);

  StreamReader& operator>>(int16_t& v);

  StreamReader& operator>>(uint16_t& v);

  StreamReader& operator>>(int32_t& v);

  StreamReader& operator>>(uint32_t& v);

  StreamReader& operator>>(int64_t& v);

  StreamReader& operator>>(uint64_t& v);

  StreamReader& operator>>(std::chrono::milliseconds& v);

  StreamReader& operator>>(std::chrono::microseconds& v);

  StreamReader& operator>>(float& v);

  StreamReader& operator>>(double& v);

  StreamReader& operator>>(char& v);

  template <int N>
  StreamReader& operator>>(char (&v)[N]) {
    ReadFixedLength(v, N);
    return *this;
  }

  template <std::size_t N>
  StreamReader& operator>>(std::array<char, N>& v) {
    ReadFixedLength(v.data(), static_cast<int>(N));
    return *this;
  }

  // N.B. Cannot allow for reading to a arbitrary char pointer as the
  //      length cannot be verified.  Also it would overshadow the
  //      char[N] input operator.
  // StreamReader& operator>>(char * v);

  StreamReader& operator>>(std::string& v);

  // Input operators for optional fields.

  StreamReader& operator>>(optional<bool>& v);

  StreamReader& operator>>(optional<int8_t>& v);

  StreamReader& operator>>(optional<uint8_t>& v);

  StreamReader& operator>>(optional<int16_t>& v);

  StreamReader& operator>>(optional<uint16_t>& v);

  StreamReader& operator>>(optional<int32_t>& v);

  StreamReader& operator>>(optional<uint32_t>& v);

  StreamReader& operator>>(optional<int64_t>& v);

  StreamReader& operator>>(optional<uint64_t>& v);

  StreamReader& operator>>(optional<float>& v);

  StreamReader& operator>>(optional<double>& v);

  StreamReader& operator>>(optional<std::chrono::milliseconds>& v);

  StreamReader& operator>>(optional<std::chrono::microseconds>& v);

  StreamReader& operator>>(optional<char>& v);

  StreamReader& operator>>(optional<std::string>& v);

  template <std::size_t N>
  StreamReader& operator>>(optional<std::array<char, N>>& v) {
    ReadFixedLength(v.data(), static_cast<int>(N));  // *** FIX ME ***
    return *this;
  }

  /// \brief Terminate current row and advance to next one.
  void EndRow();

  /// \brief Skip the data in the next columns.
  /// If the number of columns exceeds the columns remaining on the
  /// current row then skipping is terminated - it does _not_ continue
  /// skipping columns on the next row.
  /// Skipping of columns still requires the use 'EndRow' even if all
  /// remaining columns were skipped.
  /// \return Number of columns actually skipped.
  int64_t SkipColumns(int64_t num_columns_to_skip);

  /// \brief Skip the data in the next rows.
  /// Skipping of rows is not allowed if reading of data for the
  /// current row is not finished.
  /// Skipping of rows will be terminated if the end of file is
  /// reached.
  /// \return Number of rows actually skipped.
  int64_t SkipRows(int64_t num_rows_to_skip);

 protected:
  template <typename ReaderType, typename T, typename U = T>
  void Read(T* v) {
    const auto& node = nodes_[column_index_];
    auto reader = static_cast<ReaderType*>(column_readers_[column_index_++].get());
    int16_t def_level;
    int16_t rep_level;
    int64_t values_read;

    reader->ReadBatch(1, &def_level, &rep_level, reinterpret_cast<U*>(v), &values_read);

    if (values_read != 1) {
      throw ParquetException("Failed to read value for column '" + node->name() + "'");
    }
  }

  template <typename T>
  void Read(T* v) {
    const auto& node = nodes_[column_index_];
    auto reader = static_cast<Int32Reader*>(column_readers_[column_index_++].get());
    int16_t def_level;
    int16_t rep_level;
    int32_t tmp;
    int64_t values_read;

    reader->ReadBatch(1, &def_level, &rep_level, &tmp, &values_read);

    if (values_read == 1) {
      *v = tmp;
    } else {
      throw ParquetException("Failed to read value for column '" + node->name() + "'");
    }
  }

  template <typename ReaderType, typename T, typename U = T>
  void ReadOptional(optional<T>& v) {
    const auto& node = nodes_[column_index_];
    auto reader = static_cast<ReaderType*>(column_readers_[column_index_++].get());
    int16_t def_level;
    int16_t rep_level;
    U tmp;
    int64_t values_read;

    reader->ReadBatch(1, &def_level, &rep_level, &tmp, &values_read);

    if (values_read == 1) {
      v = T(tmp);
    } else if (values_read == 0) {
      v.reset();
    } else {
      throw ParquetException("Failed to read column '" + node->name() + "'");
    }
  }

  template <typename T>
  void ReadOptional(optional<T>& v) {
    const auto& node = nodes_[column_index_];
    auto reader = static_cast<Int32Reader*>(column_readers_[column_index_++].get());
    int16_t def_level;
    int16_t rep_level;
    int32_t tmp;
    int64_t values_read;

    reader->ReadBatch(1, &def_level, &rep_level, &tmp, &values_read);

    if (values_read == 1) {
      v = T(tmp);
    } else if (values_read == 0) {
      v.reset();
    } else {
      throw ParquetException("Failed to read column '" + node->name() + "'");
    }
  }

  void ReadFixedLength(char* ptr, int len);

  void Read(ByteArray* v);

  void Read(FixedLenByteArray* v);

  bool ReadOptional(ByteArray* v);

  bool ReadOptional(FixedLenByteArray* v);

  void NextRowGroup();

  void CheckColumn(Type::type physical_type, ConvertedType::type converted_type,
                   int length = 0);

  void SkipRowsInColumn(ColumnReader* reader, int64_t num_rows_to_skip);

  void SetEof();

 private:
  using node_ptr_type = std::shared_ptr<schema::PrimitiveNode>;

  std::unique_ptr<ParquetFileReader> file_reader_;
  std::shared_ptr<FileMetaData> file_metadata_;
  std::shared_ptr<RowGroupReader> row_group_reader_;
  std::vector<std::shared_ptr<ColumnReader>> column_readers_;
  std::vector<node_ptr_type> nodes_;

  bool eof_{true};
  int row_group_index_{0};
  int column_index_{0};
  int64_t current_row_{0};
  int64_t row_group_row_offset_{0};
};

PARQUET_EXPORT
StreamReader& operator>>(StreamReader&, EndRowType);

}  // namespace parquet

#endif  // PARQUET_STREAM_READER_H
