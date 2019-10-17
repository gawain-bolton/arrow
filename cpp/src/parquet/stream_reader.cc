#include "parquet/stream_reader.h"


namespace parquet {

StreamReader::StreamReader(std::unique_ptr<ParquetFileReader>& File_Reader)
  : file_reader_{ File_Reader.release() }
{
    file_metadata_ = file_reader_->metadata();

    auto * const schema{ file_metadata_->schema() };
    auto * const group_node{ schema->group_node() };

    nodes_.resize( schema->num_columns() );

    for (auto i = 0;
         i < schema->num_columns();
         ++i)
    {
        nodes_[i] = std::static_pointer_cast<schema::PrimitiveNode>(group_node->field(i));
    }
    nextRowGroup_();
}

StreamReader& StreamReader::operator>>(bool& v)
{
    checkColumn_(Type::BOOLEAN,
                 LogicalType::NONE);
    return read_<BoolReader>(v);
}

StreamReader& StreamReader::operator>>(std::int32_t& v)
{
    checkColumn_(Type::INT32,
                 LogicalType::INT_32);
    return read_<Int32Reader>(v);
}

StreamReader& StreamReader::operator>>(std::uint32_t& v)
{
    checkColumn_(Type::INT32,
                 LogicalType::UINT_32);
    return operator>>( reinterpret_cast<std::int32_t&>(v) );
}

StreamReader& StreamReader::operator>>(std::int64_t& v)
{
    checkColumn_(Type::INT64,
                 LogicalType::INT_64);
    return read_<Int64Reader>(v);
}

StreamReader& StreamReader::operator>>(std::uint64_t& v)
{
    checkColumn_(Type::INT64,
                 LogicalType::UINT_64);
    return operator>>( reinterpret_cast<std::int64_t&>(v) );
}

StreamReader& StreamReader::operator>>(float& v)
{
    checkColumn_(Type::FLOAT,
                 LogicalType::NONE);
    return read_<FloatReader>(v);
}

StreamReader& StreamReader::operator>>(double& v)
{
    checkColumn_(Type::DOUBLE,
                 LogicalType::NONE);
    return read_<DoubleReader>(v);
}

StreamReader& StreamReader::operator>>(char& v)
{
    checkColumn_(Type::FIXED_LEN_BYTE_ARRAY,
                 LogicalType::NONE,
                 1);

    FixedLenByteArray flba;

    read_(flba);

    v = static_cast<char>(flba.ptr[0]);

    return *this;
}

StreamReader& StreamReader::operator>>(std::string& v)
{
    checkColumn_(Type::BYTE_ARRAY,
                 LogicalType::UTF8);

    ByteArray ba;

    read_(ba);
    v = std::string(reinterpret_cast<const char *>(ba.ptr),
                    ba.len);

    return *this;
}

StreamReader& StreamReader::readFixedLength_(char *    ptr,
                                             const int len)
{
    checkColumn_(Type::FIXED_LEN_BYTE_ARRAY,
                 LogicalType::NONE,
                 len);

    FixedLenByteArray flba;

    read_(flba);

    std::memcpy(ptr,
                flba.ptr,
                len);

    return *this;
}

void StreamReader::read_(ByteArray& v)
{
    auto * const reader{ static_cast<ByteArrayReader *>(column_readers_[column_index_++].get()) };
    std::int64_t values_read;

    reader->ReadBatch(1,
                      nullptr,
                      nullptr,
                      &v,
                      &values_read);
}

void StreamReader::read_(FixedLenByteArray& v)
{
    auto * const reader{ static_cast<FixedLenByteArrayReader *>(column_readers_[column_index_++].get()) };
    std::int64_t      values_read;

    reader->ReadBatch(1,
                      nullptr,
                      nullptr,
                      &v,
                      &values_read);
}

void StreamReader::nextRow()
{
    column_index_ = 0;

    if ( !column_readers_[column_index_]->HasNext() )
    {
        nextRowGroup_();
    }
}

void StreamReader::nextRowGroup_()
{
    if ( row_group_index_ < file_metadata_->num_row_groups() )
    {
        row_group_reader_ = file_reader_->RowGroup(row_group_index_);
        ++row_group_index_;

        column_readers_.resize( file_metadata_->num_columns() );

        for (int i = 0; i < file_metadata_->num_columns(); ++i)
        {
            column_readers_[i] = row_group_reader_->Column(i);
        }
    }
    else
    {
        eof_ = true;
        file_reader_.reset();
        file_metadata_.reset();
        row_group_reader_.reset();
        column_readers_.clear();
    }
}

void StreamReader::checkColumn_(const Type::type        physical_type,
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

}  // namespace parquet
