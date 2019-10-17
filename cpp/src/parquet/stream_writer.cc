#include "parquet/stream_writer.h"


namespace parquet {

std::int64_t StreamWriter::default_row_group_size_{512 * 1024 * 1024}; // 512 Mio

StreamWriter::StreamWriter(std::unique_ptr<ParquetFileWriter>& File_Writer)
  : file_writer_{ File_Writer.release() }
{
    auto * const schema{ file_writer_->schema() };
    auto * const group_node{ schema->group_node() };

    nodes_.resize( schema->num_columns() );

    for (auto i = 0;
         i < schema->num_columns();
         ++i)
    {
        nodes_[i] = std::static_pointer_cast<schema::PrimitiveNode>(group_node->field(i));
    }
    newRowGroup();
}

StreamWriter::~StreamWriter()
{
    closeRowGroup_();

    if ( file_writer_ )
    {
        file_writer_->Close();
    }
}

void StreamWriter::SetDefaultMaxRowGroupSize(const std::int64_t max_size)
{
    default_row_group_size_ = max_size;
}

void StreamWriter::SetMaxRowGroupSize(const std::int64_t max_size)
{
    max_row_group_size_ = max_size;
}

StreamWriter& StreamWriter::operator<<(const bool v)
{
    checkColumn_(Type::BOOLEAN,
                 LogicalType::NONE);
    return write_<BoolWriter>(v);
}

StreamWriter& StreamWriter::operator<<(const std::int32_t v)
{
    checkColumn_(Type::INT32,
                 LogicalType::INT_32);
    return write_<Int32Writer>(v);
}

StreamWriter& StreamWriter::operator<<(const std::uint32_t v)
{
    checkColumn_(Type::INT32,
                 LogicalType::UINT_32);
    return operator<<( static_cast<std::int32_t>(v) );
}

StreamWriter& StreamWriter::operator<<(const std::int64_t v)
{
    checkColumn_(Type::INT64,
                 LogicalType::INT_64);
    return write_<Int64Writer>(v);
}

StreamWriter& StreamWriter::operator<<(const std::uint64_t v)
{
    checkColumn_(Type::INT64,
                 LogicalType::UINT_64);
    return operator<<( static_cast<std::int64_t>(v) );
}

StreamWriter& StreamWriter::operator<<(const std::chrono::milliseconds& v)
{
    checkColumn_(Type::INT64,
                 LogicalType::TIMESTAMP_MILLIS);
    return write_<Int64Writer>(v.count());
}

StreamWriter& StreamWriter::operator<<(const std::chrono::microseconds& v)
{
    checkColumn_(Type::INT64,
                 LogicalType::TIMESTAMP_MICROS);
    return write_<Int64Writer>(v.count());
}

StreamWriter& StreamWriter::operator<<(const float v)
{
    checkColumn_(Type::FLOAT,
                 LogicalType::NONE);
    return write_<FloatWriter>(v);
}

StreamWriter& StreamWriter::operator<<(const double v)
{
    checkColumn_(Type::DOUBLE,
                 LogicalType::NONE);
    return write_<DoubleWriter>(v);
}

StreamWriter& StreamWriter::operator<<(const char v)
{
    return writeFixedLength_(&v,
                             1);
}

StreamWriter& StreamWriter::operator<<(const char * v)
{
    return writeVariableLength_(v,
                                std::strlen(v));
}

StreamWriter& StreamWriter::operator<<(const std::string& v)
{
    return writeVariableLength_(v.data(),
                                v.size());
}

void StreamWriter::checkColumn_(const Type::type        physical_type,
                                const LogicalType::type logical_type,
                                const int               length)
{
    if ( static_cast<std::size_t>(column_index_) >= nodes_.size() )
    {
        throw ParquetException("Column index out-of-bounds.  Number of columns[" + std::to_string(nodes_.size()) + "] current column index[" + std::to_string(column_index_) + "]");
    }
    const auto& node{ nodes_[column_index_] };

    if ( physical_type != node->physical_type() )
    {
        throw ParquetException("Column physical type mismatch.  Column[" + node->name() + "] physical type[" + TypeToString(node->physical_type()) + "] given physical type[" + TypeToString(physical_type) + "]");
    }
    if ( logical_type != node->logical_type() )
    {
        throw ParquetException("Column logical type mismatch.  Column[" + node->name() + "] logical type[" + LogicalTypeToString(node->logical_type()) + "] given logical type[" + LogicalTypeToString(logical_type) + "]");
    }
    // Length must be exact.
    // A shorter length fixed array is not acceptable as it would
    // result in array bound read errors.
    //
    if ( length != node->type_length() )
    {
        throw ParquetException("Column length mismatch.  Column[" + node->name() + "] length[" + std::to_string(node->type_length()) + "] given length[" + std::to_string(length) + "]");
    }
}

StreamWriter& StreamWriter::writeVariableLength_(const char *      data_ptr,
                                                 const std::size_t data_len)
{
    checkColumn_(Type::BYTE_ARRAY,
                 LogicalType::UTF8);

    auto * const writer{ static_cast<ByteArrayWriter *>(row_group_writer_->column(column_index_++)) };

    ByteArray ba_value;

    ba_value.ptr = reinterpret_cast<const std::uint8_t *>(data_ptr);
    ba_value.len = data_len;

    writer->WriteBatch(1,
                       nullptr,
                       nullptr,
                       &ba_value);

    if ( max_row_group_size_ > 0 )
    {
        row_group_size_ += writer->EstimatedBufferedValueBytes();
    }
    return *this;
}

StreamWriter& StreamWriter::writeFixedLength_(const char *      data_ptr,
                                              const std::size_t data_len)
{
    checkColumn_(Type::FIXED_LEN_BYTE_ARRAY,
                 LogicalType::NONE,
                 data_len);

    auto * const writer{ static_cast<FixedLenByteArrayWriter *>(row_group_writer_->column(column_index_++)) };

    FixedLenByteArray flba_value;

    flba_value.ptr = reinterpret_cast<const std::uint8_t *>(data_ptr);

    writer->WriteBatch(1,
                       nullptr,
                       nullptr,
                       &flba_value);

    if ( max_row_group_size_ > 0 )
    {
        row_group_size_ += writer->EstimatedBufferedValueBytes();
    }
    return *this;
}

void StreamWriter::nextRow()
{
    column_index_ = 0;

    if ( max_row_group_size_ > 0 )
    {
        if ( row_group_size_ > max_row_group_size_ )
        {
            newRowGroup();
        }
        // Initialize for each row with size already written
        // (compressed + uncompressed).
        //
        row_group_size_ = row_group_writer_->total_bytes_written() + row_group_writer_->total_compressed_bytes();
    }
}

void StreamWriter::newRowGroup()
{
    closeRowGroup_();
    row_group_writer_.reset( file_writer_->AppendBufferedRowGroup() );
}

void StreamWriter::closeRowGroup_()
{
    if ( row_group_writer_ )
    {
        row_group_writer_->Close();
        row_group_writer_.reset();
    }
}

}  // namespace parquet
