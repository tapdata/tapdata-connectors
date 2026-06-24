#pragma once
// Minimal protobuf (proto3) wire-format writer for tapdata.ReadLogPayload
// (ObPlugInDataSource.proto). The ob-log-decoder linked libprotobuf and the
// generated ObPlugInDataSource.pb.cc; the obcdc-helper instead hand-encodes
// the handful of fields it needs so the binary only depends on libobcdc.
// The frames are decoded on the Java side with the existing generated
// io.tapdata.data.ob.* classes (same .proto), so the contract is preserved
// exactly.
//
// Field numbers (see ObPlugInDataSource.proto):
//   Value:          column_name=1, oneof datum { value_int=2 value_float=3
//                   value_double=4 value_string=5 value_bytes=6 is_null=7 }
//   ReadLogPayload: dbname=1 tbname=2 op=3 before=4(repeated Value)
//                   after=5(repeated Value) ddl=6 transaction_time=7
#include <string>
#include <vector>
#include <cstdint>

// tapdata.ReadLogOp values (see ObPlugInDataSource.proto).
enum ObReadLogOp
{
    OB_OP_UNKNOWN = 0,
    OB_OP_ROLLBACK = 1,
    OB_OP_COMMIT = 2,
    OB_OP_INSERT = 3,
    OB_OP_UPDATE = 4,
    OB_OP_DELETE = 5,
    OB_OP_DDL = 6,
    OB_OP_HEARTBEAT = 7
};

namespace pbw
{
    inline void write_varint(std::string& out, uint64_t v)
    {
        while (v >= 0x80) {
            out.push_back((char)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        out.push_back((char)(v & 0x7F));
    }

    inline void write_tag(std::string& out, uint32_t field, uint32_t wire_type)
    {
        write_varint(out, ((uint64_t)field << 3) | wire_type);
    }

    inline void write_varint_field(std::string& out, uint32_t field, uint64_t v)
    {
        write_tag(out, field, 0);
        write_varint(out, v);
    }

    inline void write_bytes_field(std::string& out, uint32_t field, const char* data, size_t size)
    {
        write_tag(out, field, 2);
        write_varint(out, size);
        out.append(data, size);
    }

    inline void write_string_field(std::string& out, uint32_t field, const std::string& s)
    {
        write_bytes_field(out, field, s.data(), s.size());
    }
}

// Plain holder mirroring tapdata.Value. The oneof case is tracked explicitly
// so the chosen field is always emitted (even when empty), matching proto3
// oneof semantics.
struct ValuePod
{
    enum Kind { KIND_STRING, KIND_BYTES, KIND_NULL };

    std::string columnName;
    Kind kind = KIND_NULL;
    std::string datum;

    void serialize(std::string& out) const
    {
        if (!columnName.empty())
            pbw::write_string_field(out, 1, columnName);
        switch (kind) {
            case KIND_STRING:
                pbw::write_bytes_field(out, 5, datum.data(), datum.size());
                break;
            case KIND_BYTES:
                pbw::write_bytes_field(out, 6, datum.data(), datum.size());
                break;
            case KIND_NULL:
                pbw::write_varint_field(out, 7, 1);
                break;
        }
    }
};

// Plain payload holder mirroring tapdata.ReadLogPayload. proto3 default
// values (0 / empty) are simply not emitted, which the generated Java parser
// reads back as the same defaults.
struct ReadLogPayloadPod
{
    std::string dbname;
    std::string tbname;
    int32_t op = 0; // tapdata.ReadLogOp, 0 = UNKNOWN (omitted)
    std::vector<ValuePod> before;
    std::vector<ValuePod> after;
    std::string ddl;
    int64_t transactionTime = 0;

    std::string serialize() const
    {
        std::string out;
        if (!dbname.empty())
            pbw::write_string_field(out, 1, dbname);
        if (!tbname.empty())
            pbw::write_string_field(out, 2, tbname);
        if (op != 0)
            pbw::write_varint_field(out, 3, (uint64_t)op);
        std::string member;
        for (size_t i = 0; i < before.size(); i++) {
            member.clear();
            before[i].serialize(member);
            pbw::write_bytes_field(out, 4, member.data(), member.size());
        }
        for (size_t i = 0; i < after.size(); i++) {
            member.clear();
            after[i].serialize(member);
            pbw::write_bytes_field(out, 5, member.data(), member.size());
        }
        if (!ddl.empty())
            pbw::write_string_field(out, 6, ddl);
        if (transactionTime != 0)
            pbw::write_varint_field(out, 7, (uint64_t)transactionTime);
        return out;
    }
};
