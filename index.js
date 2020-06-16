const mysql = require("mysql");
const md5 = require("blueimp-md5");
const {v3: uuid} = require("uuid");

const options = {
    //自动修复数据表
    autoRepairTable: false,
    mysql: {
        timezone: 'UTC',
        dateStrings: [
            'DATE',
            'DATETIME'
        ],
        host: '',
        user: '',
        password: '',
        database: '',
        port: '3306',
        multipleStatements: true,
        connectionLimit: 50,
        charset: 'utf8mb4'
    }
};

const failedRes = (message) => {
    return {
        code:false,
        message,
        data: ''
    };
};

const successRes = (data) => {
    return {
        code:true,
        message:'',
        data
    }
};

let pool = null;

// 组装SQL
const assembleWhereStmt = (condition) => {
    let whereStmt = '';
    let whereParams = [];
    if (condition) {
        if (typeof(condition) === 'string') {
            whereStmt = condition;
        } else {
            let whereStmts = [];
            Object.getOwnPropertyNames(condition).forEach(parameter => {
                whereStmts.push(`${parameter}=?`);
                whereParams.push(condition[parameter]);
            });
            whereStmt = whereStmts.join(' and ');
        }
    }
    return {whereStmt, whereParams};
};

const assembleInsertStmt = (tableName, entity) => {
    const fields = [];
    const valuePlaceholders = [];
    const params = [];
    Object.keys(entity).forEach(field => {
        fields.push(field);
        params.push(entity[field]);
        valuePlaceholders.push('?');
    });
    const sqlStmt = `insert into ${tableName} (${fields.join(',')}) value (${valuePlaceholders.join(',')})`;
    return {sqlStmt, params};
};

const assembleUpdateStmt = (tableName, entity, condition) => {
    const fields = [];
    const params = [];
    Object.keys(entity).forEach(field => {
        fields.push(`${field}=?`);
        params.push(entity[field]);
    });
    const {whereStmt, whereParams} = assembleWhereStmt(condition);
    const sqlStmt = `update ${tableName} set ${fields.join(',')} where ${whereStmt}`;
    return {sqlStmt, params: params.concat(whereParams)};
};

const assembleSelectStmt = (tableName, condition, fields = '*', limitStmt = null) => {
    if (fields instanceof Array) {
        fields = fields.join(',');
    }
    const {whereStmt, whereParams: params} = assembleWhereStmt(condition);
    let sqlStmt = `select ${fields} from ${tableName} ${whereStmt && 'where ' + whereStmt || ''}`;
    if (limitStmt) {
        sqlStmt = `${sqlStmt} limit ${limitStmt}`;
    }
    return {sqlStmt, params};
};

const assembleCountStmt = (tableName, condition) => {
    let {sqlStmt, params} = assembleSelectStmt(tableName, condition);
    sqlStmt = sqlStmt.replace(/select\s+\*\s+from\s+/, "select count(*) as amount from ");
    return {sqlStmt, params};
};

const getTableNameBySqlStmt = (sqlStmt) => {
    const regExpUpdateCase = /\s*update\s+([^\s]+)\s+.*/gmui;
    const updateCaseResult = regExpUpdateCase.exec(sqlStmt);
    if (updateCaseResult) {
        return updateCaseResult[1];
    }

    const regExpInsertCase = /\s*insert\s+into\s+([^\s]+)\s+.*/gmui;
    const insertCaseResult = regExpInsertCase.exec(sqlStmt);
    if (insertCaseResult) {
        return insertCaseResult[1];
    }

    return "";
};

// 错误处理
const handleError = async (err, sqlStmt) => {
    const errMessage = err.sqlMessage;

    const tableName = getTableNameBySqlStmt(sqlStmt);

    const regExpDataTooLong = /Data too long for column '([^']+)'.*/gmui;
    const regExpMaximumRowSize = /.*Row\s+size\s+too\s+large\.\s*The\s+maximum\s+row\s+size\s+for\s+the\s+used\s+table.*/;

    const dataTooLongResult = regExpDataTooLong.exec(errMessage);
    if (dataTooLongResult) {
        const columnName = dataTooLongResult[1];
        return await handleTooLongDataError(tableName, columnName);
    } else if (regExpMaximumRowSize.test(errMessage)) {
        return await handleMaximumRowSizeError(tableName);
    }
    return false;
};

const handleTooLongDataError = async (tableName, fieldName) => {
    return await increaseFieldSize(tableName, fieldName);
};

const handleMaximumRowSizeError = async (tableName) => {
    const database = options.mysql.database;
    const sqlStmt = `select column_name as field_name, column_comment as field_comment from information_schema.columns where table_schema = '${database}' and table_name = '${tableName}' and data_type = 'varchar' order by character_maximum_length desc limit 0, 1`;
    const record = await firstBySqlStmt(sqlStmt);
    if (record) {
        const {field_name, field_comment} = record;
        const sqlAlterStmt = `alter table ${tableName} modify ${field_name} text comment '${field_comment}'`;
        const {code} = await execute(sqlAlterStmt);
        return code;
    }
    return false;
};

const increaseFieldSize = async (tableName, fieldName) => {
    const databaseName = options.mysql.database;
    const sqlStmt = `select data_type as field_type, character_maximum_length as field_size, column_comment as field_comment from information_schema.columns where table_schema = '${databaseName}' and table_name = '${tableName}' and column_name = '${fieldName}'`;
    const {code, data} = await execute(sqlStmt);
    if (code) {
        const {field_type, field_size, field_comment} = data[0];
        let fieldInfo = null;
        if (field_type >= 1024) {
            if (field_type === 'varchar' || field_type === 'char') {
                fieldInfo = 'text';
            } else if (field_type === 'text') {
                fieldInfo = 'mediumtext';
            } else if (field_type === 'mediumtext') {
                fieldInfo = 'longtext';
            }
        } else {
            fieldInfo = `varchar(${field_size * 2})`;
        }

        if (fieldInfo) {
            const sqlAltStmt = `alter table ${tableName} modify ${fieldName} ${fieldInfo} comment '${field_comment}'`;
            const {code} = await execute(sqlAltStmt);
            return code;
        }
    }
    return false;
};

const tableFieldCommentToNameMap = async (tableName) => {
    const sqlStmt = `show full columns from ${tableName}`;
    const {code, data} = await execute(sqlStmt);
    if (code) {
        const map = {};
        for (const {Comment, Field} of data) {
            if (Comment && Field) {
                map[Comment] = Field;
            }
        }
        return map;
    }
    return {};
};

const tableFieldNameToCommentMap = async (tableName) => {
    const commentToNameMap = await tableFieldNameToCommentMap(tableName);
    const map = {};
    Object.keys(commentToNameMap).forEach(fieldComment => {
        const fieldName = nameToCommentMap[fieldComment];
        map[fieldName] = fieldComment;
    });
    return map;
};

/**
 *
 * @param tableFile
 * @param fileUrl
 * @param referUrl
 * @param assignedFileName
 * @returns {Promise<string>}
 */
const saveFile = async (tableFile, fileUrl, referUrl, assignedFileName = null) => {
    // 删除`#`后面的内容
    fileUrl = fileUrl.replace(/#.*/, '');

    //查看文件后缀
    const regexp = /.*?\/(([^\.\/]+)\.([^\.\?#]+))((?:\?|#).*?)?$/gmiu;
    const matchRst = regexp.exec(fileUrl);
    if (matchRst) {
        const file_type = matchRst[3];
        const file_name = assignedFileName || md5(fileUrl) + '.' + file_type;
        const guid = uuid(fileUrl, uuid.DNS);
        const options = `{"curl_options": {"10016": "${referUrl}", "10018": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"}}`;
        const entity = {
            guid,
            file_name,
            file_type,
            options,
            original_url: fileUrl
        };

        const isExisted = await has(tableFile, {guid});
        if (!isExisted) {
            await save(tableFile, entity);
        }
        return file_name;
    }
    return '';
};

// 配置
const config = (customOptions) => {
    const {mysql:mysqlOptions} = Object.assign(options, customOptions);
    //
    pool = mysql.createPool(mysqlOptions);
    pool.on('connection', conn => {
        conn.query("SET time_zone='+00:00';", error => {
            if (error) {
                throw error;
            }
        });
    });
};

const has = async (tableName, condition) => {
    const {whereStmt, whereParams} = assembleWhereStmt(condition);
    const sqlStmt = `select * from ${tableName} where ${whereStmt}`;
    return await hasBySqlStmt(sqlStmt, whereParams);
};

const hasBySqlStmt = async (sqlStmt, params = null) => {
    const {code, data} = await execute(sqlStmt, params);
    return code && data && data.length > 0;
};

const save = async (tableName, entity) => {
    const {sqlStmt, params} = assembleInsertStmt(tableName, entity);
    return await saveBySqlStmt(sqlStmt, params);
};

const saveBySqlStmt = async (sqlStmt, params = null) => {
    const {code, data} = await execute(sqlStmt, params);
    return code && data && data.insertId;
};

const update = async (tableName, entity, condition) => {
    const {sqlStmt, params} = assembleUpdateStmt(tableName, entity, condition);
    return await updateBySqlStmt(sqlStmt, params);
};

const updateBySqlStmt = async (sqlStmt, params = null) => {
    const {code} = await execute(sqlStmt, params);
    return code;
};

const saveOrUpdate = async (tableName, entity, condition) => {
    if (condition instanceof Array && condition.length > 0) {
        const isExisted = await has(tableName, condition);
        if (isExisted) {
            return await update(tableName, entity, condition);
        } else {
            return await save(tableName, entity);
        }
    }
    return false;
};

const all = async (tableName, condition, fields = '*', limitStmt = null) => {
    const {sqlStmt, params} = assembleSelectStmt(tableName, condition, fields, limitStmt);
    return await allBySqlStmt(sqlStmt, params);
};

const allBySqlStmt = async (sqlStmt, params = null) => {
    const {code, data} = await execute(sqlStmt, params);
    return code && data;
};

const first = async (tableName, condition, field = '*') => {
    let {sqlStmt, params} = assembleSelectStmt(tableName, condition, field, "0,1");
    return await firstBySqlStmt(sqlStmt, params);
};

const firstBySqlStmt = async (sqlStmt, params = null) => {
    const regex = /\s+limIt\s+\d+\s*,\s*\d+/gmui;
    if (!regex.test(sqlStmt)) {
        sqlStmt = sqlStmt + ' limit 0,1';
    }
    const {code, data} = await execute(sqlStmt, params);
    return code && data && data[0];
};

const amount = async (tableName, condition) => {
    const {sqlStmt, params} = assembleCountStmt(tableName, condition);
    return await amountBySqlStmt(sqlStmt, params);
};

const amountBySqlStmt = async (sqlStmt, params = null) => {
    const {code, data} = await execute(sqlStmt, params);
    return code && data && data[0].amount ||  0;
};

const getTableComment = async (tableName) => {
    const database = options.mysql.database;
    const sqlStmt = `select i.table_comment as comment from information_schema.tables i where i.table_schema = '${database}' and i.table_name = '${tableName}'`;
    const record = await firstBySqlStmt(sqlStmt);
    return record && record['comment'];
};

const hasField = async (tableName, fieldName) => {
    const database = options.mysql.database;
    const sqlStmt = `select count(*) as amount from information_schema.columns where table_schema = '${database}' and table_name = '${tableName}' and column_name = '${fieldName}'`;
    const amount = await amountBySqlStmt(sqlStmt);
    return amount > 0;
};

const hasTable = async (tableName) => {
    const database = options.mysql.database;
    const sqlStmt = `select count(*) as amount from information_schema.tables where table_schema = '${database}' and table_type = 'BASE TABLE' and table_name = '${tableName}'`;
    const amount = await amountBySqlStmt(sqlStmt);
    return amount > 0;
};

const renameTable = async (originalName, newName) => {
    const sqlStmt = `rename table ${originalName} to ${newName}`;
    const {code} = await execute(sqlStmt);
    return code;
};

const execute = async (sql, params = null) => {
    return new Promise((resolve, reject) => {
        return pool.getConnection((err, connection) => {
            if (err) {
                resolve(failedRes(err.message));
            } else {
                connection.query(sql, params, async (err, data, fields) => {
                    connection.release();
                    if (err) {
                        if (options.autoRepairTable) {
                            const result = await handleError(err, sql);
                            if (result) {
                                const data = await execute(sql, params);
                                resolve(data);
                            } else {
                                resolve(failedRes(err.message));
                            }
                        } else {
                            resolve(failedRes(err.message));
                        }
                    } else {
                        resolve(successRes(data));
                    }
                });
            }
        });
    });
};

const endPool = () => {
    pool.end();
};

module.exports = {
    config,
    endPool,
    execute,
    has,
    hasBySqlStmt,
    update,
    updateBySqlStmt,
    save,
    saveBySqlStmt,
    saveOrUpdate,
    all,
    allBySqlStmt,
    first,
    firstBySqlStmt,
    amount,
    amountBySqlStmt,
    tableFieldNameToCommentMap,
    tableFieldCommentToNameMap,
    saveFile,
    getTableComment,
    hasField,
    hasTable,
    renameTable,
};
