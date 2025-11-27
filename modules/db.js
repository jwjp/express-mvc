// modules/db.js

const mysql = require('mysql2/promise');
const {createLogger} = require("./logger");
require('dotenv').config();

let pool = null;

// logging - !SELECT
const logQuery = (query, params) => {
  if (!query.trim().toUpperCase().startsWith('SELECT')) {
    createLogger('query').info(mysql.format(query, params));
  }
};

// connection wrapper
const wrapConnection = (conn) => {
  if (conn.isWrapped) {
    return conn;
  }

  const originalExecute = conn.execute.bind(conn);
  conn.execute = async (...args) => {
    const [query, params] = args;
    logQuery(query, params);
    return originalExecute(...args);
  };

  conn.isWrapped = true;
  return conn;
};

const getPool = () => {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      user: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      connectionLimit: 10,
      waitForConnections: true,
      enableKeepAlive: true,
      charset: 'utf8mb4',
      timezone: '+09:00',
      dateStrings: true
    });
  }
  return pool;
};

const db = async (sql, params = []) => {
  const conn = await getPool().getConnection();
  const wrappedConn = wrapConnection(conn);
  try {
    const [rows] = await wrappedConn.execute(sql, params);
    return rows;
  } finally {
    conn.release();
  }
};

const transaction = async (callback) => {
  const conn = await getPool().getConnection();
  const wrappedConn = wrapConnection(conn);
  await wrappedConn.beginTransaction();

  try {
    const result = await callback(wrappedConn);
    await wrappedConn.commit();
    return result;
  } catch (err) {
    await wrappedConn.rollback();
    throw err;
  } finally {
    conn.release();
  }
};

module.exports = { db, transaction };