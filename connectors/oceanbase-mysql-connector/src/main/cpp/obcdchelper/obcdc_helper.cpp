// obcdc-helper: per-task subprocess that drives libobcdc directly and streams
// tapdata.ReadLogPayload frames ("4-byte big-endian length + protobuf body")
// to its original stdout. It replaces the ob-log-decoder gRPC server: the
// Java connector extracts this binary from the jar, spawns one process per
// CDC task, feeds the configuration through stdin (key=value lines, EOF ends
// the config; credentials never appear on argv) and stops it with SIGTERM.
//
// Special config keys (underscore prefix, consumed by the helper itself):
//   _task_id             per-task identifier (conf file name suffix)
//   _start_timestamp     CDC start time, epoch seconds
//   _conf_template       path of the installed libobcdc.conf template
//   _launch_timeout_sec  abort if init+launch exceeds this (default 300)
// Every other key is written into the generated per-task libobcdc conf
// (replace-or-append), e.g. cluster_user / cluster_password /
// rootserver_list / tb_white_list / memory_limit.
//
// Build (requires obcdc module headers + libobcdc.so.4):
//   g++ -std=c++14 -O2 -D_GLIBCXX_USE_CXX11_ABI=0 -DOB_USE_DRCMSG ...
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

// NOTE: no <iostream>/<sstream> here. libobcdc.so statically bundles and
// *exports* its own libstdc++ copy (std::cin, ios_base::Init, ...); symbol
// interposition then mixes two iostream states and crashes. Plain stdio is
// used for all I/O instead.
#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "libobcdc.h"
#include "MD.h"
#include "MsgWrapper.h"
#include "ob_errno.h"
#include "ob_proto_writer.h"

using namespace oceanbase::libobcdc;
using namespace oceanbase::common;

static std::atomic<bool> g_stop(false);
static volatile bool g_launch_ok = false;
static int g_data_fd = -1;
// Private copy of the original stderr: libobcdc redirects fd 2 into its own
// log/libobcdc.log.stderr during init, which would silently swallow our logs.
static int g_log_fd = 2;

static void log_line(const char* fmt, ...)
{
    char buf[1024];
    char ts[32];
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);
    int n = snprintf(buf, sizeof(buf), "[obcdc-helper] %s ", ts);
    va_list ap;
    va_start(ap, fmt);
    n += vsnprintf(buf + n, sizeof(buf) - (size_t)n - 1, fmt, ap);
    va_end(ap);
    if (n > (int)sizeof(buf) - 2) n = (int)sizeof(buf) - 2;
    buf[n++] = '\n';
    ssize_t w = write(g_log_fd, buf, (size_t)n);
    (void)w;
}

static void sig_handler(int sig)
{
    // init/launch cannot be interrupted gracefully; before launch completes a
    // stop request simply terminates the process (same as ob-log-decoder).
    if (g_launch_ok) {
        g_stop.store(true);
    } else {
        _exit(143);
    }
    (void)sig;
}

static void on_obcdc_error(const ObCDCError& err)
{
    log_line("obcdc error: level=%d errno=%d msg=%s", err.level_, err.errno_,
             err.errmsg_ == NULL ? "" : err.errmsg_);
}

static bool write_all(int fd, const void* buf, size_t len)
{
    const char* p = (const char*)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            log_line("write failed, errno=%d (%s)", errno, strerror(errno));
            return false;
        }
        p += n;
        len -= (size_t)n;
    }
    return true;
}

// Frame = 4-byte big-endian body length + serialized ReadLogPayload. Lengths
// are explicit (the ob-log-decoder used strlen() on protobuf bytes, which
// truncates at the first NUL byte).
static bool write_frame(int fd, const std::string& body)
{
    uint32_t n = htonl((uint32_t)body.size());
    return write_all(fd, &n, sizeof(n)) && write_all(fd, body.data(), body.size());
}

static bool is_ascii(const char* data, size_t len)
{
    for (size_t i = 0; i < len; i++) {
        if (data[i] <= 0 || data[i] > 127) return false;
    }
    return true;
}

static void unix_time_to_str(char* strTime, size_t cap)
{
    // strTime like "2021-05-26 11:50:10" is already formatted, not unix time
    if (strchr(strTime, ':') != NULL) return;
    time_t t = (time_t)atol(strTime);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(strTime, cap, "%Y-%m-%d %H:%M:%S", &tmv);
}

// Mirrors ob-log-decoder appendData. Returns 0: value_string, 1: value_bytes,
// 2: null.
static int append_data(std::string& field, const char* data, size_t len, int colType)
{
    char dataBuf[80];
    if (data == NULL || len <= 0) return 2;
    int ret = 0;
    switch (colType) {
        case DRCMSG_TYPE_BLOB:
        case DRCMSG_TYPE_LONG_BLOB:
        case DRCMSG_TYPE_MEDIUM_BLOB:
        case DRCMSG_TYPE_TINY_BLOB:
            snprintf(dataBuf, sizeof(dataBuf), "<lobCol,len:%lu>", (unsigned long)len);
            field.append(dataBuf);
            break;
        case DRCMSG_TYPE_TIMESTAMP: {
            // with enable_convert_timestamp_to_unix_timestamp=1, mysql-mode
            // timestamp data arrives as unix time and is converted back
            size_t n = len < sizeof(dataBuf) - 1 ? len : sizeof(dataBuf) - 1;
            memcpy(dataBuf, data, n);
            dataBuf[n] = 0;
            unix_time_to_str(dataBuf, sizeof(dataBuf));
            field.append(dataBuf);
            break;
        }
        default:
            if (!is_ascii(data, len)) ret = 1;
            field.append(data, len);
            break;
    }
    return ret;
}

static void fill_values(std::vector<ValuePod>& dest, int colCount, ICDCRecord* r,
                        ITableMeta* tableMeta, IStrArray* strBuf, binlogBuf* blogBuf)
{
    const char* data = NULL;
    size_t dataLen = 0;
    for (int i = 0; i < colCount; i++) {
        ValuePod v;
        IColMeta* colMeta = tableMeta->getCol(i);
        v.columnName = colMeta->getName();
        std::string field;
        int ret;
        if (r->isParsedRecord()) {
            strBuf->elementAt(i, data, dataLen);
            ret = append_data(field, data, dataLen, colMeta->getType());
        } else {
            ret = append_data(field, blogBuf[i].buf, blogBuf[i].buf_used_size, colMeta->getType());
        }
        if (ret == 0) {
            v.kind = ValuePod::KIND_STRING;
            v.datum.swap(field);
        } else if (ret == 1) {
            v.kind = ValuePod::KIND_BYTES;
            v.datum.swap(field);
        } else {
            v.kind = ValuePod::KIND_NULL;
        }
        dest.push_back(v);
    }
}

static bool read_file(const std::string& path, std::string& out)
{
    FILE* fp = fopen(path.c_str(), "rb");
    if (fp == NULL) return false;
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        out.append(buf, n);
    }
    fclose(fp);
    return true;
}

// Replace the value of `key` in conf (line-based "key=value"); append the
// line when the key is absent.
static void set_conf_item(std::string& conf, const std::string& key, const std::string& value)
{
    std::string out;
    bool replaced = false;
    size_t pos = 0;
    while (pos < conf.size()) {
        size_t nl = conf.find('\n', pos);
        size_t end = (nl == std::string::npos) ? conf.size() : nl;
        std::string line = conf.substr(pos, end - pos);
        pos = (nl == std::string::npos) ? conf.size() : nl + 1;
        if (!line.empty() && line[line.size() - 1] == '\r') line.erase(line.size() - 1);
        if (!replaced) {
            size_t p = line.find_first_not_of(" \t");
            if (p != std::string::npos && line.compare(p, key.size(), key) == 0) {
                size_t q = line.find_first_not_of(" \t", p + key.size());
                if (q != std::string::npos && line[q] == '=') {
                    out += key + "=" + value + "\n";
                    replaced = true;
                    continue;
                }
            }
        }
        out += line + "\n";
    }
    if (!replaced) out += key + "=" + value + "\n";
    conf.swap(out);
}

int main()
{
    prctl(PR_SET_PDEATHSIG, SIGTERM); // die with the engine JVM
    signal(SIGTERM, sig_handler);
    signal(SIGINT, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    // Keep a private fd for data frames and point fd 1 at stderr so stray
    // prints from libobcdc can never corrupt the framing. Also keep a private
    // copy of stderr for our own logs (libobcdc re-points fd 2 at init time).
    g_data_fd = dup(STDOUT_FILENO);
    g_log_fd = dup(STDERR_FILENO);
    if (g_log_fd < 0) g_log_fd = 2;
    dup2(STDERR_FILENO, STDOUT_FILENO);

    std::string task_id = "task";
    uint64_t start_timestamp = 0;
    std::string conf_template = "/home/admin/oceanbase/etc/libobcdc.conf";
    long launch_timeout = 300;
    std::map<std::string, std::string> overrides;

    char* lbuf = NULL;
    size_t lcap = 0;
    ssize_t llen;
    while ((llen = getline(&lbuf, &lcap, stdin)) != -1) {
        std::string line(lbuf, (size_t)llen);
        while (!line.empty() && (line[line.size() - 1] == '\n' || line[line.size() - 1] == '\r')) {
            line.erase(line.size() - 1);
        }
        if (line.empty() || line[0] == '#') continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);
        if (key == "_task_id") task_id = value;
        else if (key == "_start_timestamp") start_timestamp = strtoull(value.c_str(), NULL, 10);
        else if (key == "_conf_template") conf_template = value;
        else if (key == "_launch_timeout_sec") launch_timeout = atol(value.c_str());
        else overrides[key] = value;
    }
    free(lbuf);

    std::string conf;
    if (!read_file(conf_template, conf)) {
        log_line("cannot read conf template %s; is the obcdc module installed?", conf_template.c_str());
        return 2;
    }
    for (std::map<std::string, std::string>::const_iterator it = overrides.begin(); it != overrides.end(); ++it) {
        set_conf_item(conf, it->first, it->second);
    }
    std::string conf_file = "libobcdc_" + task_id + ".conf";
    FILE* fp = fopen(conf_file.c_str(), "wb");
    if (fp == NULL) {
        log_line("cannot write %s, errno=%d", conf_file.c_str(), errno);
        return 2;
    }
    fwrite(conf.data(), 1, conf.size(), fp);
    fclose(fp);
    chmod(conf_file.c_str(), 0600); // contains cluster_password

    log_line("task_id=%s start_timestamp=%llu conf=%s", task_id.c_str(),
             (unsigned long long)start_timestamp, conf_file.c_str());

    // Abort when init+launch hangs (e.g. unreachable rootserver); Java reaps
    // the exit and surfaces the stderr log.
    std::thread watchdog([launch_timeout] {
        for (long i = 0; i < launch_timeout; i++) {
            if (g_launch_ok || g_stop.load()) return;
            sleep(1);
        }
        log_line("obcdc launch not finished within %ld s, aborting", launch_timeout);
        _exit(1);
    });
    watchdog.detach();

    ObCDCFactory factory;
    IObCDCInstance* obcdc = factory.construct_obcdc();
    if (obcdc == NULL) {
        log_line("construct_obcdc failed");
        remove(conf_file.c_str());
        return 3;
    }

    int ret = obcdc->init(conf_file.c_str(), start_timestamp, &on_obcdc_error);
    if (ret != OB_SUCCESS) {
        log_line("obcdc init failed, ret=%d", ret);
        factory.deconstruct(obcdc);
        remove(conf_file.c_str());
        return 4; // the real OB error code is in the log above
    }
    log_line("init ok");

    ret = obcdc->launch();
    if (ret != OB_SUCCESS) {
        log_line("obcdc launch failed, ret=%d", ret);
        factory.deconstruct(obcdc);
        remove(conf_file.c_str());
        return 5;
    }
    g_launch_ok = true;
    log_line("launch ok, streaming records");

    const int64_t timeout_us = 1000000;
    while (!g_stop.load()) {
        ICDCRecord* r = NULL;
        ret = obcdc->next_record(&r, timeout_us);
        if (ret != OB_SUCCESS) {
            if (ret != OB_TIMEOUT) {
                log_line("next_record failed, ret=%d", ret);
            }
            continue;
        }

        unsigned int oldColCount = 0;
        unsigned int newColCount = 0;
        IStrArray* oldStrBuf = r->parsedOldCols();
        IStrArray* newStrBuf = r->parsedNewCols();
        binlogBuf* oldBinLogBuf = r->oldCols(oldColCount);
        binlogBuf* newBinLogBuf = r->newCols(newColCount);

        ITableMeta* tableMeta = NULL;
        if (r->isParsedRecord()) {
            tableMeta = new ITableMeta;
        }
        int metaRet = r->getTableMeta(tableMeta);
        int recordType = r->recordType();
        int colCount = (metaRet == 0 && (recordType <= EREPLACE || recordType == EDDL))
                       ? tableMeta->getColCount() : 0;

        bool emitted = false;
        ReadLogPayloadPod payload;
        switch (recordType) {
            case EINSERT:
                payload.op = OB_OP_INSERT;
                payload.dbname = r->dbname();
                payload.tbname = r->tbname();
                payload.transactionTime = (int64_t)r->getTimestamp();
                fill_values(payload.after, colCount, r, tableMeta, newStrBuf, newBinLogBuf);
                emitted = true;
                break;
            case EUPDATE:
                payload.op = OB_OP_UPDATE;
                payload.dbname = r->dbname();
                payload.tbname = r->tbname();
                payload.transactionTime = (int64_t)r->getTimestamp();
                fill_values(payload.after, colCount, r, tableMeta, newStrBuf, newBinLogBuf);
                fill_values(payload.before, colCount, r, tableMeta, oldStrBuf, oldBinLogBuf);
                emitted = true;
                break;
            case EDELETE:
                payload.op = OB_OP_DELETE;
                payload.dbname = r->dbname();
                payload.tbname = r->tbname();
                payload.transactionTime = (int64_t)r->getTimestamp();
                fill_values(payload.before, colCount, r, tableMeta, oldStrBuf, oldBinLogBuf);
                emitted = true;
                break;
            case EDDL:
                // newBuf's first colData is the ddl string, second is the ddl
                // schema version (timestamp)
                if (newBinLogBuf != NULL && newColCount > 0) {
                    payload.op = OB_OP_DDL;
                    payload.dbname = r->dbname();
                    payload.tbname = r->tbname();
                    payload.transactionTime = (int64_t)r->getTimestamp();
                    payload.ddl.append(newBinLogBuf[0].buf, newBinLogBuf[0].buf_used_size);
                    emitted = true;
                }
                break;
            case HEARTBEAT: {
                payload.op = OB_OP_HEARTBEAT;
                struct timeval tv;
                gettimeofday(&tv, NULL);
                payload.transactionTime = tv.tv_sec;
                emitted = true;
                break;
            }
            default:
                // EBEGIN/ECOMMIT/EROLLBACK and the rest are not forwarded
                break;
        }

        if (r->isParsedRecord()) {
            delete tableMeta;
        }
        obcdc->release_record(r);

        if (emitted) {
            std::string body = payload.serialize();
            if (!write_frame(g_data_fd, body)) {
                log_line("downstream closed, exiting");
                break;
            }
        }
    }

    log_line("stopping obcdc ...");
    obcdc->stop();
    obcdc->destroy();
    factory.deconstruct(obcdc);
    remove(conf_file.c_str());
    log_line("exit");
    return 0;
}
