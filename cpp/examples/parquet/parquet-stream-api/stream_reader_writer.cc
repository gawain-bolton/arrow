#include "arrow/io/api.h"
#include "parquet/stream_writer.h"
#include "parquet/stream_reader.h"
#include "parquet/exception.h"

#include <iostream>
#include <iomanip>
#include <ctime>
#include <cstdint>
#include <chrono>

class UserTimestamp
{
public:
  UserTimestamp() = default;

  UserTimestamp(const std::chrono::microseconds v)
    : ts_{ v }
  { }

  void dump(std::ostream& os) const
  {
      std::time_t t{ std::chrono::duration_cast<std::chrono::seconds>(ts_).count() };
      os << std::put_time(std::gmtime(&t),
                          "%Y%m%d-%H%M%S");
  }

  void dump(parquet::StreamWriter& os) const
  {
      os << ts_;
  }

private:
  std::chrono::microseconds ts_;
};


auto& operator<<(std::ostream& os,
                 const UserTimestamp& v)
{
    v.dump(os);
    return os;
}


auto& operator<<(parquet::StreamWriter& os,
                 const UserTimestamp& v)
{
    v.dump(os);
    return os;
}


auto& operator>>(parquet::StreamReader& os,
                 UserTimestamp& v)
{
    std::int64_t i;

    os >> i;
    v = UserTimestamp{ std::chrono::microseconds{i} };

    return os;
}


auto get_schema()
{
    parquet::schema::NodeVector fields;

    fields.push_back( parquet::schema::PrimitiveNode::Make("string_field1",
                                                           parquet::Repetition::REQUIRED,
                                                           parquet::Type::BYTE_ARRAY,
                                                           parquet::LogicalType::UTF8) );

    fields.push_back( parquet::schema::PrimitiveNode::Make("char[4]_field2",
                                                           parquet::Repetition::REQUIRED,
                                                           parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                           parquet::LogicalType::NONE,
                                                           4) );

    fields.push_back( parquet::schema::PrimitiveNode::Make("int32_field",
                                                           parquet::Repetition::REQUIRED,
                                                           parquet::Type::INT32,
                                                           parquet::LogicalType::INT_32) );

    fields.push_back( parquet::schema::PrimitiveNode::Make("double_field",
                                                           parquet::Repetition::REQUIRED,
                                                           parquet::Type::DOUBLE,
                                                           parquet::LogicalType::NONE) );

    // User defined timestamp
    fields.push_back( parquet::schema::PrimitiveNode::Make("timestamp_field",
                                                           parquet::Repetition::REQUIRED,
                                                           parquet::Type::INT64,
                                                           parquet::LogicalType::TIMESTAMP_MICROS) );

    return std::static_pointer_cast<parquet::schema::GroupNode>
           (parquet::schema::GroupNode::Make("schema",
                                             parquet::Repetition::REQUIRED,
                                             fields));
}

void write_parquet_file()
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_THROW_NOT_OK
    (
     arrow::io::FileOutputStream::Open("parquet-stream-example.parquet",
                                       &outfile)
    );

    parquet::WriterProperties::Builder builder;

    builder.compression(parquet::Compression::BROTLI);

    auto file_writer{ parquet::ParquetFileWriter::Open(outfile,
                                                       get_schema(),
                                                       builder.build()) };

    parquet::StreamWriter os{ file_writer };
    char char_array[4] = "ADD";

    os.SetMaxRowGroupSize(1000);

    for (auto i = 0;
         i < 200;
         ++i)
    {
        os << "Stk #" + std::to_string(i);
        os << char_array;
        os << 3 * i - 17;
        os << i * i - 2.23 * i + 2.131;
        os << UserTimestamp{ std::chrono::microseconds{1'571'237'790'919'738 + 3'000'000 * i} };
        std::cout << i << std::endl;
        os.nextRow();
    }
    std::cout << "Writing finished." << std::endl;
}

void read_parquet_file()
{
    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_THROW_NOT_OK
    (
     arrow::io::ReadableFile::Open("parquet-stream-example.parquet",
                                   &infile)
    );

    auto file_reader{ parquet::ParquetFileReader::Open(infile) };

    parquet::StreamReader os{ file_reader };

    std::string   s;
    char          char_array[4];
    std::int32_t  i;
    double        d;
    UserTimestamp ts;

    while ( !os.eof() )
    {
        os >> s;
        os >> char_array;
        os >> i;
        os >> d;
        os >> ts;
        os.nextRow();

        std::cout << s << ' '
                  << char_array << ' '
                  << i << ' '
                  << d << ' '
                  << ts << std::endl;
    }
}

int main()
{
    write_parquet_file();
    read_parquet_file();

    return 0;
}
