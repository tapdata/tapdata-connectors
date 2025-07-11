package io.tapdata.connector.mysql.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.exception.TapExLevel;
import io.tapdata.exception.TapExType;

@TapExClass(
        code = 39,
        module = "mysql-connector",
        describe = "MySQL Error Code",
        prefix = "MYSQL")
public interface MysqlErrorCode {

    @TapExCode(
            describe = "InnoDB engine:\n" +
                    "The InnoDB engine defines the row length limit through a section called \"row overflow\" data. Basically, when the line content exceeds a certain size, some data will be stored in the \"line overflow\" section. Therefore, the line length limit of InnoDB mainly depends on the file system's limit on the size of individual files, usually around 16KB (for COMPACT line format) or 32KB (for REDUNDANT and COMPACT line formats).\n" +
                    "MyISAM engine:\n" +
                    "The MyISAM engine has a fixed line length limit of 65535 bytes. This is because MyISAM indexes are implemented by calculating column length, so errors occur when column length exceeds 65535 bytes.",
            describeCN = "MySQL中的行长度限制是由于存储引擎的不同而不同。在MySQL中，最常用的存储引擎是InnoDB和MyISAM。\n" +
                    "InnoDB引擎：\n" +
                    "InnoDB引擎对行长度限制是通过一个section来定义的，这个section被称为\"行溢出\"数据。基本上，行内容超过一定大小时，会将部分数据存储在\"行溢出\"段中。因此，InnoDB的行长度限制主要取决于文件系统对单个文件大小的限制，通常这个限制大约是16KB（对于COMPACT行格式）或32KB（对于REDUNDANT和COMPACT行格式）。\n" +
                    "MyISAM引擎：\n" +
                    "MyISAM引擎有一个固定的行长度限制是65535字节。这是因为MyISAM的索引是通过计算列长度来实现的，所以当列长度超过65535字节时，就会出现错误。",
            solution = "Solution:\n" +
                    "For InnoDB engine, if you really need to store a large amount of data, consider using large data types such as TEXT, BLOB to store data larger than the row length limit.\n" +
                    "For MyISAM engine, consider the following methods:\n" +
                    "Reduce the size of a single row, such as reducing the length of VARCHAR, or not storing too long text data in columns, you can use TEXT or BLOB types.\n" +
                    "Use PARTITION BY RANGE to split the table and distribute the data to multiple tables.\n" +
                    "If it is caused by some large fields in a single row, consider splitting these fields into other tables and querying them through JOIN.\n" +
                    "In addition: The product allows unified modification of mapping types during model mapping, such as unifying VARCHAR(2000) types to TEXT types.",
            solutionCN = "解决方案：\n" +
                    "对于InnoDB引擎，如果确实需要存储大量数据，可以考虑使用TEXT、BLOB这样的大数据类型来存储大于行长度限制的数据。\n" +
                    "对于MyISAM引擎，可以考虑以下几种方法：\n" +
                    "减少单行的大小，比如减少VARCHAR的长度，或者不要将太长的文本数据存储在列中，可以使用TEXT或BLOB类型。\n" +
                    "使用PARTITION BY RANGE分割表，将数据分散到多个表中。\n" +
                    "如果是因为单行中某些大字段引起的，可以考虑将这些字段拆分到其他表中，通过JOIN来查询。\n" +
                    "另外：产品在模型映射时允许统一修改映射类型，如将VARCHAR(2000)类型统一修改为TEXT类型等。",
            dynamicDescription = "Error event: {}",
            dynamicDescriptionCN = "错误事件：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html", "https://dev.mysql.com/doc/refman/8.0/en/innodb-restrictions.html", "https://dev.mysql.com/doc/refman/8.0/en/myisam-storage-engine.html"}
    )
    String EXCEEDS_65535_LIMIT = "390009";
}
