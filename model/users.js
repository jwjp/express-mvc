// models/users.js
const {db, transaction} = require("../modules/db");

/**
 * Sample
 */
const getFooBar = async ({ limit = 10, offset = 0, userId = null } = {}) => {
  let sql = `
      SELECT
          b.id,
          b.foo_id,
          b.comment,
          b.created_at,
          f.message
      FROM bar b
               LEFT JOIN foo f ON b.foo_id = f.id
  `;

  const params = [];

  if (userId !== null) {
    sql += ' WHERE b.id = ?';
    params.push(userId);
  }

  sql += ' ORDER BY b.created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  return await db(sql, params);
};

/**
 * Sample
 */
const getTotalCount = async () => {
  const sql = `
      SELECT COUNT(*) as total
      FROM bar b
               LEFT JOIN foo f ON b.foo_id = f.id
  `;

  const [result] = await db(sql);
  return result.total;
};

/**
 * Sample
 */
const createFoobar = async (fooData, barData) => {
  return await transaction(async (conn) => {
    const [fooResult] = await conn.execute(
      'INSERT INTO foo (message) VALUES (?)',
      [fooData.message]
    );

    const [barResult] = await conn.execute(
      'INSERT INTO bar (foo_id, comment) VALUES (?, ?)',
      [fooResult.insertId, barData.comment]
    );

    return {
      fooId: fooResult.insertId,
      barId: barResult.insertId
    };
  });
};

module.exports = {
  getFooBar,
  getTotalCount,
  createFoobar
};
